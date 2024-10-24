// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.securecomputation.teesdk.cloudstorage.v1alpha

import com.google.crypto.tink.StreamingAead
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.flow
import java.util.logging.Logger
import org.wfanet.measurement.storage.StorageClient
import java.io.ByteArrayOutputStream
import java.nio.channels.ReadableByteChannel
import java.nio.ByteBuffer
import java.nio.channels.Channels
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.BlockingExecutor

/**
 * A wrapper class for the [StorageClient] interface that leverages Tink AEAD encryption/decryption
 * for blob/object storage operations on files formatted using Apache Mesos RecordIO.
 *
 * This class supports row-based encryption and decryption, enabling the processing of individual
 * rows at the client’s pace. Unlike [KmsStorageClient],
 * which encrypts entire blobs, this class focuses on handling encryption and decryption at the
 * record level inside RecordIO files.
 *
 * @param storageClient underlying client for accessing blob/object storage
 * @param dataKey a base64-encoded symmetric data key
 */
class RecordIoStorageClient(
  private val storageClient: StorageClient,
  private val streamingAead: StreamingAead,
  private val streamingAeadContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient {

/**
 * Encrypts and writes RecordIO rows to Google Cloud Storage using StreamingAead.
 *
 * This function takes a flow of RecordIO rows (represented as a Flow<ByteString>), formats each row
 * by prepending the record size and a newline character (`\n`), and encrypts the entire formatted row
 * using StreamingAead before writing the encrypted content to Google Cloud Storage.
 *
 * The function handles rows emitted at any pace, meaning it can process rows that are emitted asynchronously
 * with delays in between.
 *
 * @param blobKey The key (or name) of the blob where the encrypted content will be stored.
 * @param content A Flow<ByteString> representing the source of RecordIO rows that will be encrypted and stored.
 *
 * @return A Blob object representing the encrypted RecordIO data that was written to Google Cloud Storage.
 */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val encryptedContent = flow {
      val outputStream = ByteArrayOutputStream()
      val ciphertextChannel = this@RecordIoStorageClient.streamingAead.newEncryptingChannel(
        Channels.newChannel(outputStream), blobKey.encodeToByteArray()
      )

      content.collect { byteString ->
        val rawBytes = byteString.toByteArray()
        val recordSize = rawBytes.size.toString()
        val fullRecord = recordSize + "\n" + String(rawBytes, Charsets.UTF_8)
        val fullRecordBytes = fullRecord.toByteArray(Charsets.UTF_8)
        val buffer = ByteBuffer.wrap(fullRecordBytes)

        while (buffer.hasRemaining()) {
          ciphertextChannel.write(buffer)
        }
        emit(ByteString.copyFrom(outputStream.toByteArray()))
        outputStream.reset()
      }
      ciphertextChannel.close()
    }
   val wrappedBlob: StorageClient.Blob = storageClient.writeBlob(blobKey, encryptedContent)
    logger.fine { "Wrote encrypted content to storage with blobKey: $blobKey" }
    return RecordioBlob(wrappedBlob, blobKey)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is not decrypted until [RecordioBlob.read]
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { RecordioBlob(it, blobKey) }
  }

  /** A blob that will decrypt the content when read */
  private inner class RecordioBlob(private val blob: StorageClient.Blob, private val blobKey: String) :
    StorageClient.Blob {
    override val storageClient = this@RecordIoStorageClient.storageClient

    override val size: Long
      get() = blob.size

    /**
     * This method handles the decryption of data from Google Cloud Storage in real-time, streaming chunks
     * of encrypted data and decrypting them on-the-fly using a StreamingAead instance.
     *
     * The function then reads each row from an Apache Mesos RecordIO file and emits each one individually.
     *
     * @return The number of bytes read from the encrypted stream and written into the buffer.
     *         Returns -1 when the end of the stream is reached.
     *
     * @throws java.io.IOException If there is an issue reading from the stream or during decryption.
     */
    override fun read(): Flow<ByteString> = flow {
      val chunkChannel = Channel<ByteString>(capacity = Channel.UNLIMITED)

      CoroutineScope(streamingAeadContext).launch {
        blob.read().collect { chunk ->
          chunkChannel.send(chunk)
        }
        chunkChannel.close()
      }

      val plaintextChannel = this@RecordIoStorageClient.streamingAead.newDecryptingChannel(
        object : ReadableByteChannel {
          private var currentChunk: ByteString? = null
          private var bufferOffset = 0

          override fun isOpen(): Boolean = true

          override fun close() {}

          override fun read(buffer: ByteBuffer): Int {
            if (currentChunk == null || bufferOffset >= currentChunk!!.size()) {
              currentChunk = runBlocking {
                chunkChannel.receiveCatching().getOrNull()
              }
              if (currentChunk == null) return -1
              bufferOffset = 0
            }

            val nextChunkBuffer = currentChunk!!.asReadOnlyByteBuffer()
            nextChunkBuffer.position(bufferOffset)
            val bytesToRead = minOf(buffer.remaining(), nextChunkBuffer.remaining())
            nextChunkBuffer.limit(bufferOffset + bytesToRead)
            buffer.put(nextChunkBuffer)
            bufferOffset += bytesToRead
            return bytesToRead
          }
        },
        blobKey.encodeToByteArray()
      )

      val byteBuffer = ByteBuffer.allocate(4096)
      val sizeBuffer = ByteArrayOutputStream()

      while (true) {
        byteBuffer.clear()
        if (plaintextChannel.read(byteBuffer) <= 0) break
        byteBuffer.flip()
        while (byteBuffer.hasRemaining()) {
          val b = byteBuffer.get().toInt().toChar()

          if (b == '\n') {
            val recordSize = sizeBuffer.toString().trim().toInt()
            sizeBuffer.reset()
            val recordData = ByteBuffer.allocate(recordSize)
            while (recordData.hasRemaining()) {
              if (plaintextChannel.read(recordData) <= 0) break
            }
            recordData.flip()
            emit(ByteString.copyFrom(recordData.array()))
          } else {
            sizeBuffer.write(b.code)
          }
        }
      }
    }

    override suspend fun delete() = blob.delete()

  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}

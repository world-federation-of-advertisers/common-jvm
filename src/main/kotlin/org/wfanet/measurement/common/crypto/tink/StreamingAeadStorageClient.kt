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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.StreamingAead
import com.google.protobuf.ByteString
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.storage.StorageClient

/**
 * A wrapper class for the [StorageClient] interface that leverages Tink AEAD encryption/decryption
 * for blob/object storage operations.
 *
 * This class provides streaming encryption and decryption of data using StreamingAead, enabling
 * secure storage of large files by processing them in chunks.
 *
 * @param storageClient underlying client for accessing blob/object storage
 * @param streamingAead the StreamingAead instance used for encryption/decryption
 * @param streamingAeadContext coroutine context for encryption/decryption operations
 */
class StreamingAeadStorageClient(
  private val storageClient: StorageClient,
  private val streamingAead: StreamingAead,
  private val streamingAeadContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient {

  /**
   * Encrypts and writes data to storage using StreamingAead.
   *
   * This function takes a flow of data chunks, encrypts them using StreamingAead, and writes the
   * encrypted content to storage.
   *
   * @param blobKey The key (or name) of the blob where the encrypted content will be stored.
   * @param content A Flow<ByteString> representing the source data that will be encrypted and
   *   stored.
   * @return A Blob object representing the encrypted data that was written to storage.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val encryptedContent = flow {
      val outputStream = ByteArrayOutputStream()
      val ciphertextChannel = streamingAead.newEncryptingChannel(
        Channels.newChannel(outputStream), blobKey.encodeToByteArray()
      )
      content.collect { byteString ->
        byteString.asReadOnlyByteBufferList().forEach { buffer ->
          while (buffer.hasRemaining()) {
            ciphertextChannel.write(buffer)
          }
        }
        if (outputStream.size() > 0) {
          emit(ByteString.copyFrom(outputStream.toByteArray()))
          outputStream.reset()
        }
      }
      ciphertextChannel.close()
      if (outputStream.size() > 0) {
        emit(ByteString.copyFrom(outputStream.toByteArray()))
      }
    }

    val wrappedBlob: StorageClient.Blob = storageClient.writeBlob(blobKey, encryptedContent)
    logger.fine { "Wrote encrypted content to storage with blobKey: $blobKey" }
    return EncryptedBlob(wrappedBlob, blobKey)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is not decrypted until [EncryptedBlob.read]
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { EncryptedBlob(it, blobKey) }
  }

  /** A blob that will decrypt the content when read */
  private inner class EncryptedBlob(
    private val blob: StorageClient.Blob,
    private val blobKey: String,
  ) : StorageClient.Blob {
    override val storageClient = this@StreamingAeadStorageClient.storageClient

    override val size: Long
      get() = blob.size

    /**
     * Reads and decrypts the blob's content.
     *
     * This method handles the decryption of data by collecting all encrypted data first, then
     * decrypting it as a single operation.
     *
     * @return A Flow of ByteString containing the decrypted data.
     * @throws java.io.IOException If there is an issue reading from the stream or during
     *   decryption.
     */
    override fun read(): Flow<ByteString> = flow {
      val chunkChannel = Channel<ByteString>(capacity = Channel.UNLIMITED)

      blob.read().collect { chunk -> chunkChannel.send(chunk) }
      chunkChannel.close()

      val plaintextChannel =
        this@StreamingAeadStorageClient.streamingAead.newDecryptingChannel(
          object : ReadableByteChannel {
            private var currentChunk: ByteString? = null
            private var bufferOffset = 0

            override fun isOpen(): Boolean = true

            override fun close() {}

            override fun read(buffer: ByteBuffer): Int {
              if (currentChunk == null || bufferOffset >= currentChunk!!.size()) {
                currentChunk = runBlocking { chunkChannel.receiveCatching().getOrNull() }
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
          blobKey.encodeToByteArray(),
        )

      val buffer = ByteBuffer.allocate(8192)
      while (true) {
        buffer.clear()
        val bytesRead = plaintextChannel.read(buffer)
        if (bytesRead <= 0) break
        buffer.flip()
        emit(ByteString.copyFrom(buffer.array(), 0, buffer.limit()))
      }
    }

    override suspend fun delete() = blob.delete()
  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}

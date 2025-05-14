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
import java.nio.channels.ClosedChannelException
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.CoroutineReadableByteChannel
import org.wfanet.measurement.common.CoroutineWritableByteChannel
import org.wfanet.measurement.common.asFlow
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
   * Writes an encrypted flow of data to storage using a specified blob key. This function encrypts
   * each element of the [content] flow and streams it to the underlying storage client. The
   * encryption is performed by wrapping a [CoroutineWritableByteChannel] in an encrypting channel,
   * allowing for non-blocking, coroutine-friendly I/O.
   *
   * @param blobKey The key for the blob in storage, used as associated data in encryption.
   * @param content A [Flow] of [ByteString] elements, each representing a chunk of data to be
   *   encrypted and stored.
   * @return A [StorageClient.Blob] object representing the stored encrypted blob.
   * @throws ClosedChannelException if the channel is closed before all data is written.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val encryptedContent = channelFlow {
      val writableChannel = CoroutineWritableByteChannel(channel)

      streamingAead.newEncryptingChannel(writableChannel, blobKey.encodeToByteArray()).use {
        ciphertextChannel ->
        content.collect { byteString ->
          byteString.asReadOnlyByteBufferList().forEach { buffer ->
            while (buffer.hasRemaining()) {
              val bytesWritten = ciphertextChannel.write(buffer)
              if (bytesWritten == 0) {
                yield()
                continue
              }
            }
          }
        }
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

  override suspend fun listBlobNames(prefix: String, delimiter: String): List<String> {
    return storageClient.listBlobNames(prefix, delimiter)
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
      val scope = CoroutineScope(streamingAeadContext)
      try {
        val chunkChannel = blob.read().produceIn(scope)
        val readableChannel = CoroutineReadableByteChannel(chunkChannel)

        val plaintextChannel =
          streamingAead.newDecryptingChannel(readableChannel, blobKey.encodeToByteArray())
        emitAll(plaintextChannel.asFlow(BYTES_PER_MIB, streamingAeadContext))
      } finally {
        scope.cancel()
      }
    }

    override suspend fun delete() = blob.delete()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.crypto.tink.Aead
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.storage.StorageClient

/**
 * A wrapper class for [StorageClient] interface that uses AEAD encryption/decryption for
 * blob/object storage operations.
 *
 * @param storageClient underlying client for accessing blob/object storage
 * @param aead Tink AEAD integration specific encrypt/decrypt
 * @param aeadContext context for [aead] operations, containing a
 *   [kotlinx.coroutines.CoroutineDispatcher] that supports blocking calls
 */
class KmsStorageClient
internal constructor(
  private val storageClient: StorageClient,
  private val aead: Aead,
  private val aeadContext: @BlockingExecutor CoroutineContext,
) : StorageClient {

  /**
   * Creates a blob with the specified [blobKey] and [content] encrypted by [aead].
   *
   * @param blobKey the key for the blob. This is used as the AEAD associated data.
   * @param content [Flow] producing the content be encrypted and stored in the blob
   * @return [StorageClient.Blob] with [content] encrypted by [aead]
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    logger.fine { "Creating ciphertext for KmsStorageClient" }
    val ciphertext: ByteArray =
      withContext(aeadContext) { aead.encrypt(content.toByteArray(), blobKey.encodeToByteArray()) }
    logger.fine { "Created ciphertext. Writing ciphertext to storage $blobKey." }
    val wrappedBlob: StorageClient.Blob =
      storageClient.writeBlob(blobKey, ciphertext.toByteString())
    logger.fine { "Wrote ciphertext to storage $blobKey" }
    return AeadBlob(wrappedBlob, blobKey)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is not decrypted until [AeadBlob.read]
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { AeadBlob(it, blobKey) }
  }

  override suspend fun listBlobs(prefix: String?): List<StorageClient.Blob> {
    return storageClient.listBlobs(prefix)
  }

  /** A blob that will decrypt the content when read */
  private inner class AeadBlob(private val blob: StorageClient.Blob, override val blobKey: String) :
    StorageClient.Blob {
    override val storageClient = this@KmsStorageClient.storageClient

    override val size: Long
      get() = blob.size

    override fun read() = flow {
      logger.fine { "Reading ciphertext from KmsStorageClient $blobKey" }
      val ciphertext = blob.read().toByteArray()
      logger.fine { "Decrypting KmsStorageClient ciphertext $blobKey" }
      val plaintext =
        withContext(aeadContext) { aead.decrypt(ciphertext, blobKey.encodeToByteArray()) }
      logger.fine { "Finished reading plaintext from KmsStorageClient $blobKey" }
      emit(plaintext.toByteString())
    }

    override suspend fun delete() = blob.delete()
  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}

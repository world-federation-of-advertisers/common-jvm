// Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * A wrapper class for the [StorageClient] interface that leverages Tink AEAD encryption/decryption
 * for blob/object storage operations.
 *
 * This class provides encryption and decryption of data using Aead.
 *
 * @param storageClient underlying client for accessing blob/object storage. Must implement
 *   [ConditionalOperationStorageClient] since this wrapper exposes conditional-write methods.
 * @param aead the Aead instance used for encryption/decryption
 */
class AeadStorageClient(
  private val storageClient: ConditionalOperationStorageClient,
  private val aead: Aead,
) : StorageClient, ConditionalOperationStorageClient {

  /**
   * Writes an encrypted flow of data to storage using a specified blob key.
   *
   * @param blobKey The key for the blob in storage, used as associated data in encryption.
   * @param content A [Flow] of [ByteString] elements, each representing a chunk of data to be
   *   encrypted and stored.
   * @return A [StorageClient.Blob] object representing the stored encrypted blob.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val wrappedBlob: StorageClient.Blob =
      storageClient.writeBlob(blobKey, encrypt(blobKey, content))
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

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return storageClient.listBlobs(prefix)
  }

  override suspend fun getFreshnessToken(blobKey: String): String? =
    storageClient.getFreshnessToken(blobKey)

  override suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    val wrappedBlob: StorageClient.Blob =
      storageClient.writeBlobIfUnchanged(blobKey, freshnessToken, encrypt(blobKey, content))
    logger.fine { "Wrote encrypted content via writeBlobIfUnchanged(token): $blobKey" }
    return EncryptedBlob(wrappedBlob, blobKey)
  }

  override suspend fun writeBlobIfNotFound(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    val wrappedBlob: StorageClient.Blob =
      storageClient.writeBlobIfNotFound(blobKey, encrypt(blobKey, content))
    logger.fine { "Wrote encrypted content via writeBlobIfNotFound: $blobKey" }
    return EncryptedBlob(wrappedBlob, blobKey)
  }

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob is EncryptedBlob) { "Incompatible blob type" }
    val wrappedBlob: StorageClient.Blob =
      storageClient.writeBlobIfUnchanged(blob.underlying, encrypt(blob.blobKey, content))
    logger.fine { "Wrote encrypted content via writeBlobIfUnchanged: ${blob.blobKey}" }
    return EncryptedBlob(wrappedBlob, blob.blobKey)
  }

  /**
   * Encrypts [content] under [blobKey] as associated data. AEAD is single-shot (not streaming), so
   * the entire content is flattened to bytes before encryption.
   */
  private suspend fun encrypt(blobKey: String, content: Flow<ByteString>): Flow<ByteString> {
    val associatedData: ByteString = blobKey.toByteStringUtf8()
    val ciphertext: ByteString =
      aead.encrypt(content.flatten().toByteArray(), associatedData.toByteArray()).toByteString()
    return flow { emit(ciphertext) }
  }

  /** A blob that will decrypt the content when read */
  private inner class EncryptedBlob(
    val underlying: StorageClient.Blob,
    override val blobKey: String,
  ) : StorageClient.Blob {

    override val storageClient = this@AeadStorageClient.storageClient

    override val size: Long
      get() = underlying.size

    override val createTime: java.time.Instant
      get() = underlying.createTime

    override val updateTime: java.time.Instant
      get() = underlying.updateTime

    /**
     * Reads and decrypts the underlying's content.
     *
     * This method handles the decryption of data by collecting all encrypted data first, then
     * decrypting it as a single operation.
     *
     * @return A Flow of ByteString containing the decrypted data.
     * @throws java.io.IOException If there is an issue reading from the stream or during
     *   decryption.
     */
    override fun read(): Flow<ByteString> = flow {
      val associatedData: ByteString = blobKey.toByteStringUtf8()
      val data = underlying.read().flatten().toByteArray()
      emit(aead.decrypt(data, associatedData.toByteArray()).toByteString())
    }

    override suspend fun delete() = underlying.delete()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    init {
      AeadConfig.register()
    }
  }
}

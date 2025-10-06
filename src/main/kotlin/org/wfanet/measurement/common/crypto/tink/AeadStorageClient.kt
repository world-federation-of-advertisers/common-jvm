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
import org.wfanet.measurement.storage.StorageClient

/**
 * A wrapper class for the [StorageClient] interface that leverages Tink AEAD encryption/decryption
 * for blob/object storage operations.
 *
 * This class provides encryption and decryption of data using Aead.
 *
 * @param storageClient underlying client for accessing blob/object storage
 * @param aead the Aead instance used for encryption/decryption
 */
class AeadStorageClient(private val storageClient: StorageClient, private val aead: Aead) :
  StorageClient {

  /**
   * Writes an encrypted flow of data to storage using a specified blob key.
   *
   * @param blobKey The key for the blob in storage, used as associated data in encryption.
   * @param content A [Flow] of [ByteString] elements, each representing a chunk of data to be
   *   encrypted and stored.
   * @return A [StorageClient.Blob] object representing the stored encrypted blob.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val associatedData: ByteString = blobKey.toByteStringUtf8()
    val ciphertext: ByteString =
      aead.encrypt(content.flatten().toByteArray(), associatedData.toByteArray()).toByteString()
    val wrappedBlob: StorageClient.Blob = storageClient.writeBlob(blobKey, ciphertext)
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

  /** A blob that will decrypt the content when read */
  private inner class EncryptedBlob(
    private val blob: StorageClient.Blob,
    override val blobKey: String,
  ) : StorageClient.Blob {
    override val storageClient = this@AeadStorageClient.storageClient

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
      val associatedData: ByteString = blobKey.toByteStringUtf8()
      val data = blob.read().flatten().toByteArray()
      emit(aead.decrypt(data, associatedData.toByteArray()).toByteString())
    }

    override suspend fun delete() = blob.delete()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    init {
      AeadConfig.register()
    }
  }
}

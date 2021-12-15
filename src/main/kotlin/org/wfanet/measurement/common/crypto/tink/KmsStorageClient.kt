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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.read

/**
 * A wrapper class for [StorageClient] interface that uses AEAD encryption/decryption for
 * blob/object storage operations.
 *
 * @param storageClient underlying client for accessing blob/object storage
 * @param aead Tink AEAD integration specific encrypt/decrypt
 */
internal class KmsStorageClient(private val storageClient: StorageClient, private val aead: Aead) :
  StorageClient {

  override val defaultBufferSizeBytes: Int
    get() = storageClient.defaultBufferSizeBytes

  /**
   * Creates a blob with the specified [blobKey] and [content] encrypted by [aead].
   *
   * @param content [Flow] producing the content be encrypted and stored in the blob
   * @return [StorageClient.Blob] with [content] encrypted by [aead]
   */
  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val ciphertext = aead.encrypt(content.toByteArray(), null)
    return storageClient.createBlob(
      blobKey,
      ciphertext.asBufferedFlow(storageClient.defaultBufferSizeBytes)
    )
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is not decrypted until [AeadBlob.read]
   */
  override fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { AeadBlob(blob) }
  }

  /** A blob that will decrypt the content when read */
  private inner class AeadBlob(private val blob: StorageClient.Blob) : StorageClient.Blob {
    override val storageClient = this@KmsStorageClient.storageClient

    override val size: Long
      get() = blob.size

    override fun read(bufferSizeBytes: Int) =
      flow {
          emit(this@KmsStorageClient.aead.decrypt(blob.read().toByteArray(), null).toByteString())
        }
        .asBufferedFlow(bufferSizeBytes)

    override fun delete() = blob.delete()
  }
}

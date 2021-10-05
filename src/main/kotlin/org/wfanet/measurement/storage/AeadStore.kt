package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.Aead
import org.wfanet.measurement.common.flatten

/**
 * Blob/object store with AEAD encryption/decryption on all writing/reading.
 *
 * @see Store
 *
 * @param aead AEAD intergation specific encrypt/decrypt
 * @param storageClient client for accessing blob/object storage
 * @param generateBlobKey generator for unique blob keys
 */
abstract class AeadStore<T>
protected constructor(
  private val aead: Aead,
  private val storageClient: StorageClient,
  generateBlobKey: BlobKeyGenerator<T>,
) : Store<T>(storageClient, generateBlobKey) {

  /**
   * Write a blob with specified [content] encrypted by [aead]
   *
   * @param context context from which to derive the blob key
   * @param content [Flow] producing the content be encrypted and written
   * @return [Blob] with a key derived from [context]
   */
  override suspend fun write(context: T, content: Flow<ByteString>): Blob {
    val encryptedContent = aead.encrypt(content.flatten())
    return super.write(
      context,
      encryptedContent.asBufferedFlow(storageClient.defaultBufferSizeBytes)
    )
  }

  /**
   * Returns a [AeadBlob] with specified blob key, or `null` if not found.
   *
   * Blob content is not decrypted until [AeadBlob.read]
   */
  override fun get(blobKey: String): Blob? {
    val blob = super.get(blobKey)
    return blob?.let { Blob(blobKey, AeadBlob(it)) }
  }

  /** A blob that will decrypt the content when read */
  inner class AeadBlob(val blob: Blob) : StorageClient.Blob {
    override val size: Long = readEncyptedBytes().size().toLong()

    override val storageClient = this@AeadStore.storageClient

    override fun read(bufferSizeBytes: Int): Flow<ByteString> {
      return this@AeadStore.aead
        .decrypt(readEncyptedBytes())
        .asBufferedFlow(storageClient.defaultBufferSizeBytes)
    }

    override fun delete() = blob.delete()

    private fun readEncyptedBytes(): ByteString {
      var data: ByteString
      runBlocking {
        data = blob.read(this@AeadStore.storageClient.defaultBufferSizeBytes).flatten()
      }
      return data
    }
  }
}

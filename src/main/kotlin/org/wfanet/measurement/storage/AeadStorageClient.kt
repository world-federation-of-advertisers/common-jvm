package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.Aead
import org.wfanet.measurement.common.flatten

/**
 * A wrapper class for [StorageClient] interface that uses AEAD encryption/decryption for
 * blob/object storage operations.
 *
 * @param aead AEAD integration specific encrypt/decrypt
 * @param storageClient underlying client for accessing blob/object storage
 */
class AeadStorageClient(val aead: Aead, val storageClient: StorageClient) : StorageClient {
  override val defaultBufferSizeBytes: Int
    get() = storageClient.defaultBufferSizeBytes

  /**
   * Creates a blob with the specified [blobKey] and [content] encrypted by [aead].
   * @param content [Flow] producing the content be encrypted and stored in the blob
   * @return [StorageClient.Blob] with [content] encrypted by [aead]
   */
  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val ciphertext = aead.encrypt(content.flatten())
    return storageClient.createBlob(
      blobKey, ciphertext.asBufferedFlow(storageClient.defaultBufferSizeBytes))
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
  inner class AeadBlob(val blob: StorageClient.Blob) : StorageClient.Blob {
    override val storageClient = this@AeadStorageClient.storageClient

    private val encryptedBytes: ByteString by lazy {
      runBlocking { blob.read(storageClient.defaultBufferSizeBytes).flatten() }
    }

    override val size: Long = encryptedBytes.size().toLong()

    override fun read(bufferSizeBytes: Int): Flow<ByteString> {
      return this@AeadStorageClient.aead
        .decrypt(encryptedBytes)
        .asBufferedFlow(storageClient.defaultBufferSizeBytes)
    }

    override fun delete() = blob.delete()
  }
}

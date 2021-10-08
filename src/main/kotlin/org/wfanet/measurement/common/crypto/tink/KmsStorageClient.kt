package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.Aead
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient

class KmsStorageClient(private val storageClient: StorageClient, private val aead: Aead) : StorageClient {
  override val defaultBufferSizeBytes: Int
    get() = storageClient.defaultBufferSizeBytes

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    TODO("Not yet implemented")
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    TODO("Not yet implemented")
  }
}

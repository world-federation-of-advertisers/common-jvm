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

package org.wfanet.measurement.storage.testing

import com.google.protobuf.ByteString
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient

/**
 * The default byte buffer size. Chosen as it is a commonly used default buffer size in an attempt
 * to keep the tests as close to actual usage as possible.
 */
private const val BYTE_BUFFER_SIZE = BYTES_PER_MIB * 1

/** In-memory [StorageClient]. */
class InMemoryStorageClient : StorageClient {
  private val storageMap = ConcurrentHashMap<String, StorageClient.Blob>()

  /** Exposes all the blobs in the [StorageClient]. */
  val contents: Map<String, StorageClient.Blob>
    get() = storageMap

  private fun deleteKey(path: String) {
    storageMap.remove(path)
  }

  override val defaultBufferSizeBytes: Int
    get() = BYTE_BUFFER_SIZE

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return withContext(Dispatchers.IO) {
      var created = false
      val blob =
        storageMap.getOrPut(blobKey) {
          created = true
          Blob(blobKey, runBlocking { content.flatten() })
        }
      require(created) { "Blob with key $blobKey already exists" }
      blob
    }
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    return storageMap[blobKey]
  }

  private inner class Blob(private val blobKey: String, private val content: ByteString) :
    StorageClient.Blob {

    override val size: Long
      get() = content.size().toLong()

    override val storageClient: InMemoryStorageClient
      get() = this@InMemoryStorageClient

    override fun read(bufferSizeBytes: Int): Flow<ByteString> =
      content.asBufferedFlow(bufferSizeBytes)

    override fun delete() = storageClient.deleteKey(blobKey)
  }
}

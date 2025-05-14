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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient

/** In-memory [StorageClient]. */
class InMemoryStorageClient : StorageClient {
  private val storageMap = ConcurrentHashMap<String, StorageClient.Blob>()

  /** Exposes all the blobs in the [StorageClient]. */
  val contents: Map<String, StorageClient.Blob>
    get() = storageMap

  private fun deleteKey(path: String) {
    storageMap.remove(path)
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val blob = Blob(blobKey, content.flatten())
    storageMap[blobKey] = blob
    return blob
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return storageMap[blobKey]
  }

  override suspend fun listBlobNames(prefix: String, delimiter: String): List<String> {
    if (prefix.isEmpty()) {
      throw IllegalArgumentException("Prefix must not be empty")
    }

    val regex =
      if (delimiter.isNotEmpty()) {
        val escapedDelimiter = delimiter.replace("\\", "\\\\")
        Regex(
          "($prefix)((?!$escapedDelimiter).)*$escapedDelimiter|($prefix)((?!$escapedDelimiter).)*"
        )
      } else {
        Regex("($prefix).*")
      }

    return buildSet { addAll(storageMap.keys().toList().mapNotNull { regex.find(it)?.value }) }
      .toList()
  }

  private inner class Blob(private val blobKey: String, private val content: ByteString) :
    StorageClient.Blob {

    override val size: Long
      get() = content.size().toLong()

    override val storageClient: InMemoryStorageClient
      get() = this@InMemoryStorageClient

    override fun read(): Flow<ByteString> = flowOf(content)

    override suspend fun delete() = storageClient.deleteKey(blobKey)
  }
}

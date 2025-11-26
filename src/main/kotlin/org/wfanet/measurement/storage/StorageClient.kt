// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.StorageClient.Blob

/** Interface for blob/object storage operations. */
interface StorageClient {
  /** Writes [content] to a blob with [blobKey]. */
  suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): Blob

  /**
   * Writes [content] to a blob with [blobKey].
   *
   * Prefer the [Flow] overload if your content is not already a [ByteString] and not all in memory.
   */
  suspend fun writeBlob(blobKey: String, content: ByteString) = writeBlob(blobKey, flowOf(content))

  /** Returns a [Blob] for the specified key, or `null` if it cannot be found. */
  suspend fun getBlob(blobKey: String): Blob?

  /**
   * Lists Blobs
   *
   * @param prefix A blob key prefix. When not specified, all blobs are returned. When specified, it
   *   filters out blobs with blob keys that do not match the prefix.
   */
  suspend fun listBlobs(prefix: String? = null): Flow<Blob>

  /** Reference to a blob in a storage system. */
  interface Blob {
    /** The [StorageClient] from which this [Blob] was obtained. */
    val storageClient: StorageClient

    val blobKey: String

    /** Size of the blob in bytes. */
    val size: Long

    /** Returns a [Flow] for the blob content. */
    fun read(): Flow<ByteString>

    /** Deletes the blob. */
    suspend fun delete()
  }
}

/**
 * [StorageClient] which supports additional operations with conditional behavior for freshness
 * validation.
 */
interface ConditionalOperationStorageClient : StorageClient {
  /**
   * Writes [content] to the blob specified by the [blobKey][Blob.blobKey] of [blob] if it has not
   * changed on the backend.
   *
   * @return the updated [Blob]
   * @throws BlobChangedException if the blob was changed on the backend
   * @throws StorageException if write failed
   */
  suspend fun writeBlobIfUnchanged(blob: Blob, content: Flow<ByteString>): Blob
}

open class StorageException(message: String?, cause: Throwable? = null) : Exception(message, cause)

class BlobChangedException(message: String?, cause: Throwable? = null) :
  StorageException(message, cause)

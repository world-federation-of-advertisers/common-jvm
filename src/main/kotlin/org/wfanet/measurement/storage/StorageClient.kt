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
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
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

  /**
   * Lists blob keys and unique key prefixes (virtual directories) directly under the given [prefix]
   * using [DELIMITER].
   *
   * This mirrors the behavior of GCS delimiter-based listing:
   * - Blobs whose keys do not contain the [DELIMITER] after [prefix] are returned as-is.
   * - Blobs whose keys contain the [DELIMITER] after [prefix] are grouped, and only the common
   *   prefix up to and including the first encountered [DELIMITER] is returned (deduplicated).
   *
   * For example, given blobs:
   * ```
   *   path/file.txt
   *   path/2026-03-13/done
   *   path/2026-03-13/data
   *   path/2026-03-14/done
   * ```
   *
   * Calling `listBlobKeysAndPrefixes("path/")` returns `["path/2026-03-13/", "path/2026-03-14/",
   * "path/file.txt"]`.
   *
   * Implementations should use the storage backend's native prefix/delimiter support where
   * available (e.g., GCS delimiter listing) to avoid enumerating all objects.
   *
   * @param prefix The prefix to list under.
   * @return A [Flow] of blob keys and prefix strings ending at the first encountered [DELIMITER].
   */
  suspend fun listBlobKeysAndPrefixes(prefix: String): Flow<String> {
    val keys = mutableSetOf<String>()
    listBlobs(prefix).toList().forEach { blob ->
      val relativePath = blob.blobKey.removePrefix(prefix)
      val delimiterIndex = relativePath.indexOf(DELIMITER)
      if (delimiterIndex >= 0) {
        keys.add(prefix + relativePath.substring(0, delimiterIndex + DELIMITER.length))
      } else {
        keys.add(blob.blobKey)
      }
    }
    return flow { keys.sorted().forEach { emit(it) } }
  }

  companion object {
    /** Delimiter used by [listBlobKeysAndPrefixes] to define virtual directory boundaries. */
    const val DELIMITER = "/"
  }

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

/**
 * [StorageClient] which supports blob metadata operations.
 *
 * This is useful for storage systems that support custom metadata and lifecycle management based on
 * custom timestamps (e.g., GCS Custom-Time).
 */
interface BlobMetadataStorageClient : StorageClient {
  /**
   * Updates metadata on an existing blob.
   *
   * @param blobKey The key of the blob to update
   * @param customCreateTime Optional custom create timestamp (for lifecycle management)
   * @param metadata Custom key-value metadata pairs
   * @throws StorageException if update fails
   */
  suspend fun updateBlobMetadata(
    blobKey: String,
    customCreateTime: Instant? = null,
    metadata: Map<String, String> = emptyMap(),
  )
}

open class StorageException(message: String?, cause: Throwable? = null) : Exception(message, cause)

class BlobChangedException(message: String?, cause: Throwable? = null) :
  StorageException(message, cause)

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

/**
 * Interface for blob/object storage operations.
 *
 * It is assumed that the content of blobs accessed through this interface is immutable once the
 * blob has been created. Hence, this interface has no operations for modifying the content of an
 * existing blob.
 */
interface StorageClient {
  /**
   * Writes [content] to a blob with [blobKey].
   *
   * @throws MaxAttemptsReachedWritingException when running out of retries.
   * @throws PermanentWritingException when encountering permanent error.
   */
  suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): Blob

  /**
   * Writes [content] to a blob with [blobKey].
   *
   * Prefer the [Flow] overload if your content is not already a [ByteString] and not all in memory.
   *
   * @throws MaxAttemptsReachedWritingException when running out of retries.
   * @throws PermanentWritingException when encountering permanent error.
   */
  suspend fun writeBlob(blobKey: String, content: ByteString) = writeBlob(blobKey, flowOf(content))

  /** Returns a [Blob] for the specified key, or `null` if it cannot be found. */
  suspend fun getBlob(blobKey: String): Blob?

  /**
   * List file and folder names.
   *
   * When prefix and delimiter options are empty, all file names (blob keys) are returned. The
   * prefix option filters out the file or folder names that do not match the prefix. The delimiter
   * option (e.g. "/") are used with prefix to emulate hierarchy. When delimiter option is not
   * empty, the function returns full blob name for items that are directly under the current prefix
   * and do not contain another instance of the delimiter after the prefix part.
   */
  suspend fun listBlobNames(prefix: String, delimiter: String): List<String>

  /** Reference to a blob in a storage system. */
  interface Blob {
    /** The [StorageClient] from which this [Blob] was obtained. */
    val storageClient: StorageClient

    /** Size of the blob in bytes. */
    val size: Long

    /** Returns a [Flow] for the blob content. */
    fun read(): Flow<ByteString>

    /** Deletes the blob. */
    suspend fun delete()
  }
}

open class StorageException(message: String?, cause: Throwable? = null) : Exception(message, cause)

/**
 * [StorageException] which indicates the max attempts has been reached. [cause] reflects the
 * transient error of the last attempt.
 */
class MaxAttemptsReachedWritingException(message: String? = null, cause: Throwable? = null) :
  StorageException(message, cause)

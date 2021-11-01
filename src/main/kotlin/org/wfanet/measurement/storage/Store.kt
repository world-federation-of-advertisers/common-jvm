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

package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.asBufferedFlow

typealias BlobKeyGenerator<T> = (context: T) -> String

/**
 * Blob/object store.
 *
 * Blobs written by a given [Store] type can only be retrieved by a [Store] of that same type. This
 * is enforced by use of a private per-[Store] [blobKeyPrefix].
 *
 * @param storageClient client for accessing blob/object storage
 * @param generateBlobKey generator for unique blob keys (the key should have no slash in the
 * beginning)
 */
abstract class Store<T>
protected constructor(
  private val storageClient: StorageClient,
  private val generateBlobKey: BlobKeyGenerator<T>
) {
  /**
   * The private unique blob key prefix for this [Store]. The value should contain no slash in the
   * beginning or at the end.
   */
  protected abstract val blobKeyPrefix: String

  class Blob(val blobKey: String, clientBlob: StorageClient.Blob) :
    StorageClient.Blob by clientBlob

  /**
   * Writes a blob with the specified content.
   *
   * @param context context from which to derive the blob key
   * @param content [Flow] producing the content to write
   * @return [Blob] with a key derived from [context]
   */
  suspend fun write(context: T, content: Flow<ByteString>): Blob {
    val blobKey = generateBlobKey(context)
    val privateBlobKey = "$blobKeyPrefix/$blobKey"
    val createdBlob = storageClient.createBlob(privateBlobKey, content)
    return Blob(blobKey, createdBlob)
  }

  /** @see write */
  suspend fun write(context: T, content: ByteString): Blob =
    write(context, content.asBufferedFlow(storageClient.defaultBufferSizeBytes))

  /** Returns a [Blob] with the specified blob key, or `null` if not found. */
  fun get(blobKey: String): Blob? {
    val privateBlobKey = "$blobKeyPrefix/$blobKey"
    return storageClient.getBlob(privateBlobKey)?.let { Blob(blobKey, it) }
  }
}

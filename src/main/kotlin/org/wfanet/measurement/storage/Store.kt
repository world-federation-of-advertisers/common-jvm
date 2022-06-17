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
import kotlinx.coroutines.flow.flowOf

/**
 * Blob/object store.
 *
 * Blobs written by a given [Store] type can only be retrieved by a [Store] of that same type. This
 * is enforced by use of a private per-[Store] [blobKeyPrefix].
 *
 * @param storageClient client for accessing blob/object storage
 */
abstract class Store<in T> protected constructor(private val storageClient: StorageClient) {
  /**
   * The private unique blob key prefix for this [Store].
   *
   * This should neither begin nor end in a slash (`/`).
   */
  protected abstract val blobKeyPrefix: String

  /**
   * Deterministically derives a blob key from [context].
   *
   * Derived blob keys should be equal for equal contexts, and should not be equal for unequal
   * contexts.
   *
   * @return the blob key, without any leading or trailing slash (`/`).
   */
  protected abstract fun deriveBlobKey(context: T): String

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
    val blobKey = deriveBlobKey(context)
    val privateBlobKey = "$blobKeyPrefix/$blobKey"
    val createdBlob = storageClient.writeBlob(privateBlobKey, content)
    return Blob(blobKey, createdBlob)
  }

  /** @see write */
  suspend fun write(context: T, content: ByteString): Blob = write(context, flowOf(content))

  /** Returns a [Blob] with the specified blob key, or `null` if not found. */
  suspend fun get(blobKey: String): Blob? {
    val privateBlobKey = "$blobKeyPrefix/$blobKey"
    return storageClient.getBlob(privateBlobKey)?.let { Blob(blobKey, it) }
  }

  /** Returns a [Blob] for the specified [context], or `null` if not found. */
  suspend fun get(context: T): Blob? = get(deriveBlobKey(context))
}

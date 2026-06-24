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
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageException

/**
 * In-memory [StorageClient] with [ConditionalOperationStorageClient] and
 * [BlobMetadataStorageClient] support.
 */
class InMemoryStorageClient :
  StorageClient, ConditionalOperationStorageClient, BlobMetadataStorageClient {
  private val storageMap = ConcurrentHashMap<String, Blob>()

  /** Exposes all the blobs in the [StorageClient]. */
  val contents: Map<String, StorageClient.Blob>
    get() = storageMap

  /** Returns the custom create time set on [blobKey] via [updateBlobMetadata], or null if unset. */
  fun getCustomCreateTime(blobKey: String): Instant? = storageMap[blobKey]?.customTime

  private fun deleteKey(path: String) {
    storageMap.remove(path)
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val now = Instant.now()
    val existing = storageMap[blobKey]
    val existingCreateTime = existing?.createTime
    val nextGeneration = (existing?.generation ?: 0L) + 1L
    // A fresh write replaces any prior custom metadata, mirroring GCS upload semantics where a
    // re-upload does not carry over previously-set user metadata unless the writer sets it.
    val blob =
      Blob(
        blobKey = blobKey,
        contentBytes = content.flatten(),
        createTime = existingCreateTime ?: now,
        updateTime = now,
        generation = nextGeneration,
        customTime = null,
        metadata = emptyMap(),
      )
    storageMap[blobKey] = blob
    return blob
  }

  /**
   * Conditional write that succeeds only if [blob]'s generation matches the current generation in
   * storage.
   *
   * Note: the generation check and subsequent write are not atomic. This matches GCS semantics only
   * when there is no concurrent writer for the same key; production GCS enforces atomicity via the
   * server-side `ifGenerationMatch` precondition, but the in-memory test double does not. Safe for
   * single-coroutine-per-key test usage.
   */
  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob.storageClient === this) { "Blob does not belong to this storage client" }
    return writeBlobIfUnchanged(blob.blobKey, (blob as Blob).generation.toString(), content)
  }

  override suspend fun getFreshnessToken(blobKey: String): String? =
    storageMap[blobKey]?.generation?.toString()

  override suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    val expectedGeneration =
      freshnessToken.toLongOrNull()
        ?: throw IllegalArgumentException(
          "Invalid freshness token for InMemory: $freshnessToken (must be numeric)"
        )
    require(expectedGeneration > 0) {
      "freshnessToken must encode a positive generation, got $expectedGeneration"
    }
    val current = storageMap[blobKey] ?: throw BlobChangedException("Blob does not exist: $blobKey")
    if (current.generation != expectedGeneration) {
      throw BlobChangedException(
        "Blob $blobKey is at generation ${current.generation}, not $expectedGeneration"
      )
    }
    return writeBlob(blobKey, content)
  }

  override suspend fun writeBlobIfNotFound(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    if (storageMap.containsKey(blobKey)) {
      throw BlobChangedException("Blob already exists: $blobKey")
    }
    return writeBlob(blobKey, content)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return storageMap[blobKey]
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return flow {
      for (key in storageMap.keys().toList()) {
        if (prefix.isNullOrEmpty()) {
          emit(storageMap.getValue(key))
        } else {
          if (key.startsWith(prefix = prefix, ignoreCase = false)) emit(storageMap.getValue(key))
        }
      }
    }
  }

  override suspend fun updateBlobMetadata(
    blobKey: String,
    customCreateTime: Instant?,
    metadata: Map<String, String>,
  ) {
    require(customCreateTime != null || metadata.isNotEmpty()) {
      "At least one of customCreateTime or metadata must be specified"
    }
    val existing = storageMap[blobKey] ?: throw StorageException("Blob not found: $blobKey")
    storageMap[blobKey] =
      Blob(
        blobKey = blobKey,
        contentBytes = existing.contentBytes,
        createTime = existing.createTime,
        updateTime = Instant.now(),
        generation = existing.generation,
        customTime = customCreateTime ?: existing.customTime,
        // GCS PATCH semantics: keys in [metadata] are added or overwritten; existing keys not in
        // [metadata] are preserved. Map+ is right-biased so collisions resolve to the new value.
        metadata = existing.metadata + metadata,
      )
  }

  private inner class Blob(
    override val blobKey: String,
    val contentBytes: ByteString,
    override val createTime: Instant,
    override val updateTime: Instant,
    val generation: Long,
    val customTime: Instant?,
    override val metadata: Map<String, String>,
  ) : StorageClient.Blob {

    override val size: Long
      get() = contentBytes.size().toLong()

    override val storageClient: InMemoryStorageClient
      get() = this@InMemoryStorageClient

    override fun read(): Flow<ByteString> = flowOf(contentBytes)

    override suspend fun delete() = storageClient.deleteKey(blobKey)
  }
}

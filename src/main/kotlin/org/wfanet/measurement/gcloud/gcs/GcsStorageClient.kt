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

package org.wfanet.measurement.gcloud.gcs

import com.google.api.gax.paging.Page
import com.google.cloud.WriteChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException as GcsStorageException
import com.google.protobuf.ByteString
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.Blocking
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ObjectMetadataStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageException

/**
 * Size of byte buffer used to read blobs.
 *
 * This comes from [com.google.cloud.storage.BlobReadChannel.DEFAULT_CHUNK_SIZE].
 */
private const val READ_BUFFER_SIZE = BYTES_PER_MIB * 2

/**
 * Google Cloud Storage (GCS) implementation of [StorageClient] for a single bucket.
 *
 * @param storage GCS API client
 * @param bucketName name of the GCS bucket
 * @param blockingContext [CoroutineContext] for blocking operations
 */
class GcsStorageClient(
  private val storage: Storage,
  val bucketName: String,
  private val blockingContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient, ConditionalOperationStorageClient, ObjectMetadataStorageClient {

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    try {
      return withContext(blockingContext + CoroutineName("writeBlob")) {
        val gcsBlob: Blob = storage.create(BlobInfo.newBuilder(bucketName, blobKey).build())
        gcsBlob.write(content)
        ClientBlob(gcsBlob.reload(), blobKey)
      }
    } catch (e: GcsStorageException) {
      throw StorageException("Error writing blob with key $blobKey", e)
    }
  }

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob is ClientBlob) { "Incompatible blob type" }
    val gcsBlob: Blob = blob.blob
    return withContext(blockingContext + CoroutineName("replaceBlob")) {
      try {
        gcsBlob.write(content)
      } catch (e: GcsStorageException) {
        if (e.code == HTTP_PRECONDITION_FAILED) {
          throw BlobChangedException(
            "Precondition failed when attempting to write ${blob.blobKey}",
            e,
          )
        } else {
          throw StorageException("Error writing blob with key ${blob.blobKey}", e)
        }
      }
      ClientBlob(gcsBlob.reload(), blob.blobKey)
    }
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob: Blob =
      withContext(blockingContext + CoroutineName("getBlob")) {
        try {
          storage.get(bucketName, blobKey)
        } catch (e: GcsStorageException) {
          throw StorageException("Error getting blob with key $blobKey", e)
        }
      } ?: return null
    return ClientBlob(blob, blobKey)
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    val options = mutableListOf<Storage.BlobListOption>()

    if (!prefix.isNullOrEmpty()) {
      options.add(Storage.BlobListOption.prefix(prefix))
    }

    val blobPage: Page<Blob> =
      try {
        storage.list(bucketName, *options.toTypedArray())
      } catch (e: GcsStorageException) {
        throw StorageException("Fail to list blobs.", e)
      }

    return flow {
        for (blob: Blob in blobPage.iterateAll()) {
          emit(ClientBlob(blob, blob.name))
        }

        var nextPage: Page<Blob>? = blobPage.nextPage
        while (nextPage != null) {
          for (blob: Blob in nextPage.iterateAll()) {
            emit(ClientBlob(blob, blob.name))
          }
          nextPage = nextPage.nextPage
        }
      }
      .flowOn(blockingContext + CoroutineName("listBlobs"))
  }

  override suspend fun updateObjectMetadata(
    blobKey: String,
    customTime: Instant?,
    metadata: Map<String, String>,
  ) {
    if (customTime == null && metadata.isEmpty()) {
      return
    }

    withContext(blockingContext + CoroutineName("updateObjectMetadata")) {
      try {
        val blobId = BlobId.of(bucketName, blobKey)
        val blobInfoBuilder = BlobInfo.newBuilder(blobId)

        if (customTime != null) {
          blobInfoBuilder.setCustomTimeOffsetDateTime(
            OffsetDateTime.ofInstant(customTime, ZoneOffset.UTC)
          )
        }

        if (metadata.isNotEmpty()) {
          blobInfoBuilder.setMetadata(metadata)
        }

        val updatedBlob = storage.update(blobInfoBuilder.build())
        if (updatedBlob == null) {
          throw StorageException("Blob not found: $blobKey")
        }
      } catch (e: GcsStorageException) {
        throw StorageException("Error updating metadata for blob with key $blobKey", e)
      }
    }
  }

  /** [StorageClient.Blob] implementation for [GcsStorageClient]. */
  private inner class ClientBlob(val blob: Blob, override val blobKey: String) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@GcsStorageClient

    override val size: Long
      get() = blob.size

    override fun read(): Flow<ByteString> {
      return blob.reader().asFlow(READ_BUFFER_SIZE, blockingContext + CoroutineName("readBlob"))
    }

    override suspend fun delete() {
      check(withContext(blockingContext + CoroutineName("deleteBlob")) { blob.delete() }) {
        "Failed to delete blob ${blob.blobId}"
      }
    }
  }

  companion object {
    private const val HTTP_PRECONDITION_FAILED = 412

    /** Constructs a [GcsStorageClient] from command-line flags. */
    fun fromFlags(gcs: GcsFromFlags) = GcsStorageClient(gcs.storage, gcs.bucket)
  }
}

@Blocking
private suspend fun Blob.write(content: Flow<ByteString>) {
  writer(Storage.BlobWriteOption.generationMatch(generation)).use { byteChannel: WriteChannel ->
    content.collect { bytes ->
      for (buffer in bytes.asReadOnlyByteBufferList()) {
        while (buffer.hasRemaining() && currentCoroutineContext().isActive) {
          // Writer has its own internal buffering, so we can just use whatever buffers we
          // already have.
          if (byteChannel.write(buffer) == 0) {
            // Nothing was written, so we may have a non-blocking channel that nothing
            // can be
            // written to right now. Suspend this coroutine to avoid monopolizing the
            // thread.
            yield()
          }
        }
      }
    }
  }
}

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
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
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
) : StorageClient, ConditionalOperationStorageClient, BlobMetadataStorageClient {

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return withContext(blockingContext + CoroutineName("writeBlob")) {
      val blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build()
      try {
        // Single resumable upload, no precondition: unconditional last-writer-wins semantics. The
        // prior implementation split this into `storage.create()` + `Blob.write(generationMatch)`,
        // which introduced a small race window where a concurrent writer between the two ops
        // would cause this `writeBlob` to fail with a precondition error — surprising for what is
        // documented as an unconditional write.
        storage.writer(blobInfo).use { byteChannel -> writeChannel(byteChannel, content) }
        val writtenBlob =
          storage.get(BlobId.of(bucketName, blobKey))
            ?: throw StorageException("Blob $blobKey vanished after successful writeBlob")
        ClientBlob(writtenBlob, blobKey)
      } catch (e: GcsStorageException) {
        throw StorageException("Error writing blob with key $blobKey", e)
      }
    }
  }

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob is ClientBlob) { "Incompatible blob type" }
    val gcsBlob: Blob = blob.blob
    val expectedGeneration = gcsBlob.generation
    return withContext(blockingContext + CoroutineName("writeBlobIfUnchanged")) {
      val blobInfo = BlobInfo.newBuilder(bucketName, blob.blobKey).build()
      try {
        // Single resumable upload gated on `IfGenerationMatch=<expectedGeneration>`. Race-free:
        // the server atomically checks the precondition; either this write lands at the expected
        // generation, or it fails-fast with 412.
        storage.writer(blobInfo, Storage.BlobWriteOption.generationMatch(expectedGeneration)).use {
          byteChannel ->
          writeChannel(byteChannel, content)
        }
        val writtenBlob =
          storage.get(BlobId.of(bucketName, blob.blobKey))
            ?: throw StorageException(
              "Blob ${blob.blobKey} vanished after successful writeBlobIfUnchanged"
            )
        ClientBlob(writtenBlob, blob.blobKey)
      } catch (e: GcsStorageException) {
        if (e.code == HTTP_PRECONDITION_FAILED) {
          throw BlobChangedException(
            "Precondition failed when attempting to write ${blob.blobKey}",
            e,
          )
        }
        throw StorageException("Error writing blob with key ${blob.blobKey}", e)
      }
    }
  }

  override suspend fun writeBlobIfAbsent(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    return withContext(blockingContext + CoroutineName("writeBlobIfAbsent")) {
      val blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build()
      try {
        // Single resumable upload gated on `IfGenerationMatch=0` (blob must not exist). The
        // precondition is checked atomically on the server, so concurrent writers are race-free:
        // exactly one write lands, the others fail-fast with `412 Precondition Failed`.
        storage.writer(blobInfo, Storage.BlobWriteOption.doesNotExist()).use { byteChannel ->
          writeChannel(byteChannel, content)
        }
        val writtenBlob =
          storage.get(BlobId.of(bucketName, blobKey))
            ?: throw StorageException("Blob $blobKey vanished after successful writeBlobIfAbsent")
        ClientBlob(writtenBlob, blobKey)
      } catch (e: GcsStorageException) {
        if (e.code == HTTP_PRECONDITION_FAILED) {
          throw BlobChangedException("Precondition failed: blob $blobKey already exists", e)
        }
        throw StorageException("Error writing blob with key $blobKey", e)
      }
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
      }
      .flowOn(blockingContext + CoroutineName("listBlobs"))
  }

  override suspend fun listBlobKeysAndPrefixes(prefix: String): Flow<String> {
    val options =
      arrayOf(
        Storage.BlobListOption.prefix(prefix),
        Storage.BlobListOption.delimiter(StorageClient.DELIMITER),
      )

    val blobPage: Page<Blob> =
      try {
        storage.list(bucketName, *options)
      } catch (e: GcsStorageException) {
        throw StorageException("Failed to list blob keys.", e)
      }

    return flow {
        // When using a delimiter, the GCS API returns both items (direct blobs) and prefix
        // entries (virtual directories ending with the delimiter).
        for (blob: Blob in blobPage.iterateAll()) {
          emit(blob.name)
        }
      }
      .flowOn(blockingContext + CoroutineName("listBlobKeysAndPrefixes"))
  }

  override suspend fun updateBlobMetadata(
    blobKey: String,
    customCreateTime: Instant?,
    metadata: Map<String, String>,
  ) {
    require(customCreateTime != null || metadata.isNotEmpty()) {
      "At least one of customCreateTime or metadata must be specified"
    }

    withContext(blockingContext + CoroutineName("updateBlobMetadata")) {
      try {
        val blobId = BlobId.of(bucketName, blobKey)
        val blobInfo =
          BlobInfo.newBuilder(blobId)
            .apply {
              if (customCreateTime != null) {
                setCustomTimeOffsetDateTime(
                  OffsetDateTime.ofInstant(customCreateTime, ZoneOffset.UTC)
                )
              }

              if (metadata.isNotEmpty()) {
                setMetadata(metadata)
              }
            }
            .build()

        val updatedBlob = storage.update(blobInfo)
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

    override val createTime: Instant
      get() = checkNotNull(blob.createTimeOffsetDateTime).toInstant()

    override val updateTime: Instant
      get() = checkNotNull(blob.updateTimeOffsetDateTime).toInstant()

    /** Custom user metadata from the underlying GCS object. */
    override val metadata: Map<String, String> by lazy {
      // The Java SDK declares Blob.getMetadata as Map<String, String?> because draft BlobInfo
      // builders use `null` values as a "delete this key on PATCH" sentinel. A fetched object
      // never contains literal null values (GCS removes such keys server-side), so the
      // mapNotNull filter here is defensive against an SDK quirk that does not occur on fetched
      // blobs in practice.
      val raw: Map<String, String?> = blob.metadata ?: return@lazy emptyMap()
      raw.entries.mapNotNull { (k, v) -> v?.let { k to it } }.toMap()
    }

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

/** Streams [content] to [byteChannel], yielding the coroutine when the channel is full. */
@Blocking
private suspend fun writeChannel(byteChannel: WriteChannel, content: Flow<ByteString>) {
  content.collect { bytes ->
    for (buffer in bytes.asReadOnlyByteBufferList()) {
      while (buffer.hasRemaining() && currentCoroutineContext().isActive) {
        // Writer has its own internal buffering, so we can just use whatever buffers we already
        // have.
        if (byteChannel.write(buffer) == 0) {
          // Nothing was written, so we may have a non-blocking channel that nothing can be
          // written to right now. Suspend this coroutine to avoid monopolizing the thread.
          yield()
        }
      }
    }
  }
}

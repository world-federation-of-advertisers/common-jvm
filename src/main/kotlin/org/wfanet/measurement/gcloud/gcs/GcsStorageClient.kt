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

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
    // No precondition: unconditional last-writer-wins. Single resumable upload — the prior
    // implementation split this into `storage.create()` + `Blob.write(generationMatch)`, which
    // introduced a small race window where a concurrent writer between the two ops would cause
    // this `writeBlob` to fail with a precondition error.
    doWrite(
      blobKey = blobKey,
      content = content,
      coroutineName = "writeBlob",
      writeOptions = emptyArray(),
      preconditionMessage = null,
    )

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob is ClientBlob) { "Incompatible blob type" }
    return writeBlobAtGeneration(blob.blobKey, blob.blob.generation, content)
  }

  override suspend fun getFreshnessToken(blobKey: String): String? {
    val blob: Blob? =
      withContext(blockingContext + CoroutineName("getFreshnessToken")) {
        try {
          storage.get(BlobId.of(bucketName, blobKey))
        } catch (e: GcsStorageException) {
          throw StorageException("Error getting freshness token for blob $blobKey", e)
        }
      }
    return blob?.generation?.toString()
  }

  override suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    val expectedGeneration =
      freshnessToken.toLongOrNull()
        ?: throw IllegalArgumentException(
          "Invalid freshness token for GCS: $freshnessToken (must be a numeric generation)"
        )
    require(expectedGeneration > 0) {
      "freshnessToken must be a positive generation, got $expectedGeneration"
    }
    return writeBlobAtGeneration(blobKey, expectedGeneration, content)
  }

  override suspend fun writeBlobIfNotFound(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob = writeBlobAtGeneration(blobKey, 0L, content)

  private suspend fun writeBlobAtGeneration(
    blobKey: String,
    expectedGeneration: Long,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    val coroutineName =
      if (expectedGeneration == 0L) "writeBlobIfNotFound" else "writeBlobIfUnchanged"
    val preconditionMessage: () -> String =
      if (expectedGeneration == 0L) {
        { "Precondition failed: blob $blobKey already exists" }
      } else {
        { "Precondition failed: blob $blobKey not at expected generation $expectedGeneration" }
      }
    return doWrite(
      blobKey = blobKey,
      content = content,
      coroutineName = coroutineName,
      writeOptions = arrayOf(Storage.BlobWriteOption.generationMatch(expectedGeneration)),
      preconditionMessage = preconditionMessage,
    )
  }

  /**
   * Shared body for [writeBlob], [writeBlobIfUnchanged] and [writeBlobIfNotFound]. Single resumable
   * upload with any [writeOptions] applied; refetches the resulting blob to get its generation +
   * updateTime. Translates HTTP 412 into [BlobChangedException] iff [preconditionMessage] is
   * non-null (i.e. the caller actually set a precondition).
   */
  private suspend fun doWrite(
    blobKey: String,
    content: Flow<ByteString>,
    coroutineName: String,
    writeOptions: Array<Storage.BlobWriteOption>,
    preconditionMessage: (() -> String)?,
  ): ClientBlob =
    withContext(blockingContext + CoroutineName(coroutineName)) {
      val blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build()
      try {
        storage.writer(blobInfo, *writeOptions).use { byteChannel ->
          writeChannel(byteChannel, content)
        }
        val writtenBlob =
          storage.get(BlobId.of(bucketName, blobKey))
            ?: throw StorageException("Blob $blobKey vanished after successful $coroutineName")
        ClientBlob(writtenBlob, blobKey)
      } catch (e: GcsStorageException) {
        if (preconditionMessage != null && e.code == HTTP_PRECONDITION_FAILED) {
          throw BlobChangedException(preconditionMessage(), e)
        }
        throw StorageException("Error writing blob with key $blobKey", e)
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

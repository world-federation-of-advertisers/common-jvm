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
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException as GcsStorageException
import com.google.protobuf.ByteString
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.storage.MaxAttemptsReachedWritingException
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
  private val maxAttempts: Int = 5,
  private val retryBackoff: ExponentialBackoff = ExponentialBackoff(),
) : StorageClient {

  /** Write [content] into a blob in storage. */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    var attempt = 1
    while (true) {
      try {
        return writeBlobInternal(blobKey, content)
      } catch (e: GcsStorageException) {
        if (!e.isRetryable) {
          throw StorageException("Fail to write blob due to non-retryable error", e)
        }

        attempt += 1
        if (attempt > maxAttempts) {
          throw MaxAttemptsReachedWritingException("Reach max attempts.", e)
        } else {
          delay(retryBackoff.durationForAttempt(attempt))
        }
      }
    }
  }

  /**
   * Write blob into storage.
   *
   * @throws [StorageException] if writing is not fully complete.
   */
  private suspend fun writeBlobInternal(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob =
    withContext(blockingContext + CoroutineName("writeBlob")) {
      val blob = storage.create(BlobInfo.newBuilder(bucketName, blobKey).build())

      blob.writer().use { byteChannel ->
        content.collect { bytes ->
          for (buffer in bytes.asReadOnlyByteBufferList()) {
            while (buffer.hasRemaining() && currentCoroutineContext().isActive) {
              // Writer has its own internal buffering, so we can just use whatever buffers we
              // already have.
              if (byteChannel.write(buffer) == 0) {
                // Nothing was written, so we may have a non-blocking channel that nothing can be
                // written to right now. Suspend this coroutine to avoid monopolizing the thread.
                yield()
              }
            }
          }
        }
      }
      ClientBlob(blob.reload(), blobKey)
    }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob: Blob? =
      withContext(blockingContext + CoroutineName("getBlob")) { storage.get(bucketName, blobKey) }
    return blob?.let { ClientBlob(blob, blobKey) }
  }

  override suspend fun listBlobs(prefix: String?): List<StorageClient.Blob> {
    val options = mutableListOf<Storage.BlobListOption>()

    if (!prefix.isNullOrEmpty()) {
      options.add(Storage.BlobListOption.prefix(prefix))
    }

    val blobList: Page<Blob> =
      try {
        withContext(blockingContext + CoroutineName("listBlobs")) {
          storage.list(bucketName, *options.toTypedArray())
        }
      } catch (e: GcsStorageException) {
        throw StorageException("Fail to list blobs.", e)
      }

    return buildList {
      for (blob: Blob in blobList.iterateAll()) {
        add(ClientBlob(blob, blob.name))
      }
    }
  }

  /** [StorageClient.Blob] implementation for [GcsStorageClient]. */
  private inner class ClientBlob(private val blob: Blob,
                                 override val blobKey: String
  ) : StorageClient.Blob {
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
    /** Constructs a [GcsStorageClient] from command-line flags. */
    fun fromFlags(gcs: GcsFromFlags) = GcsStorageClient(gcs.storage, gcs.bucket)
  }
}

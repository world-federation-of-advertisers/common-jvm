// Copyright 2025 The Cross-Media Measurement Authors
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
import java.io.File
import java.net.URI
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.gcs.GcsStorageRetryConfig
import org.wfanet.measurement.gcloud.gcs.buildGcsStorageOptions
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

// Data class to store parsed information
data class BlobUri(val scheme: String, val bucket: String, val key: String)

/*
 * A [StorageClient] from a blob uri. Currently only supports Google Cloud Storage and File System.
 * If file system storage should be supported, input a root File were data should be written/read.
 * @param blobUri - the [BlobUri] from which a StorageClient can be built
 * @param rootDirectory - only needed for file system storage clients
 * @param projectId - optional if google cloud storage client needs require project id
 * @param retryConfig - resilient retry/timeout configuration for Google Cloud Storage reads;
 *   defaults to [GcsStorageRetryConfig.DEFAULT]
 */
class SelectedStorageClient(
  private val blobUri: BlobUri,
  private val rootDirectory: File? = null,
  private val projectId: String? = null,
  private val retryConfig: GcsStorageRetryConfig = GcsStorageRetryConfig.DEFAULT,
) : StorageClient, ConditionalOperationStorageClient {

  constructor(
    url: String,
    rootDirectory: File? = null,
    projectId: String? = null,
    retryConfig: GcsStorageRetryConfig = GcsStorageRetryConfig.DEFAULT,
  ) : this(parseBlobUri(url), rootDirectory, projectId, retryConfig)

  val underlyingClient: ConditionalOperationStorageClient =
    when (blobUri.scheme) {
      "s3" -> {
        throw IllegalArgumentException("S3 is not currently supported")
      }
      "gs" -> {
        val storageOptions =
          buildGcsStorageOptions(projectId = projectId, retryConfig = retryConfig)
        GcsStorageClient(storageOptions.service, requireNotNull(blobUri.bucket))
      }
      "file" -> {
        FileSystemStorageClient(
          checkNotNull(rootDirectory).toPath().resolve(blobUri.bucket).toFile()
        )
      }
      else -> throw IllegalArgumentException("Unsupported blobUrl: $blobUri")
    }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    check(blobUri.key == blobKey)
    return underlyingClient.writeBlob(blobKey, content)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    check(blobUri.key == blobKey)
    return underlyingClient.getBlob(blobKey)
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return underlyingClient.listBlobs(prefix)
  }

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob = underlyingClient.writeBlobIfUnchanged(blob, content)

  override suspend fun getFreshnessToken(blobKey: String): String? {
    check(blobUri.key == blobKey)
    return underlyingClient.getFreshnessToken(blobKey)
  }

  override suspend fun writeBlobIfUnchanged(
    blobKey: String,
    freshnessToken: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    check(blobUri.key == blobKey)
    return underlyingClient.writeBlobIfUnchanged(blobKey, freshnessToken, content)
  }

  override suspend fun writeBlobIfNotFound(
    blobKey: String,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    check(blobUri.key == blobKey)
    return underlyingClient.writeBlobIfNotFound(blobKey, content)
  }

  companion object {

    fun parseBlobUri(url: String): BlobUri {
      val uri = URI.create(url)
      return when (uri.scheme) {
        "s3" -> {
          throw IllegalArgumentException("S3 is not currently supported")
        }
        "gs" -> {
          val bucket = uri.host
          val key = uri.path.removePrefix("/")
          BlobUri(scheme = "gs", bucket = bucket, key = key)
        }
        "file" -> {
          val (bucket, key) = uri.path.removePrefix("/").split("/", limit = 2)
          BlobUri(scheme = "file", bucket = bucket, key = key)
        }
        else -> throw IllegalArgumentException("Unable to parse blob uri $url")
      }
    }
  }
}

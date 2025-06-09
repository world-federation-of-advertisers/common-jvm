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

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.ByteString
import java.io.File
import java.net.URI
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

// Data class to store parsed information
data class BlobUri(val scheme: String, val bucket: String, val key: String)

/*
 * A [StorageClient] from a blob uri. Currently only supports Google Cloud Storage and File System.
 * If file system storage should be supported, input a root File were data should be written/read.
 * @param blobUri - the [BlobUri] from which a StorageClient can be built
 * @param rootDirectory - only needed for file system storage clients
 * @param projectId - optional if google cloud storage client needs require project id
 */
class SelectedStorageClient(
  private val blobUri: BlobUri,
  private val rootDirectory: File? = null,
  private val projectId: String? = null,
) : StorageClient {

  constructor(
    url: String,
    rootDirectory: File? = null,
    projectId: String? = null,
  ) : this(parseBlobUri(url), rootDirectory, projectId)

  val underlyingClient: StorageClient =
    when (blobUri.scheme) {
      "s3" -> {
        throw IllegalArgumentException("S3 is not currently supported")
      }
      "gs" -> {
        val storageOptions =
          if (projectId != null) StorageOptions.newBuilder().setProjectId(projectId).build()
          else StorageOptions.getDefaultInstance()
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

  companion object {

    fun parseBlobUri(url: String): BlobUri {
      val uri = URI.create(url)
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~ SSC, URI: ${uri}")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~ SSC, URI2:")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~ SSC, URI schema: ${uri.scheme}")
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
          println("~~~~~~~~~~~~~~~ bucket: ${bucket}")
          println("~~~~~~~~~~~~~~~ key: ${key}")
          BlobUri(scheme = "file", bucket = bucket, key = key)
        }
        else -> throw IllegalArgumentException("Unable to parse blob uri $url")
      }
    }
  }
}

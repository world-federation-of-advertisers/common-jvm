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
import java.io.File
import java.net.URI
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

// Data class to store parsed information
data class BlobUri(val scheme: String, val bucket: String?, val path: String)

/*
 * Builds a [StorageClient] from a blob uri. Currently only supports Google Cloud Storage and File System.
 * If file system storage should be supported, input a root File were data should be written/read.
 * @blobUri - the [BlobUri] from which a StorageClient can be built
 * @rootDirectory - only needed for file system storage clients
 * @projectId - optional if google cloud storage client needs require project id
 */
class StorageClientFactory(
  private val blobUri: BlobUri,
  private val rootDirectory: File? = null,
  private val projectId: String? = null,
) {

  constructor(url: String, rootDirectory: File) : this(parseBlobUri(url), rootDirectory)

  fun build(): StorageClient {
    val storageClient: StorageClient =
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
          FileSystemStorageClient(requireNotNull(rootDirectory))
        }
        else -> throw IllegalArgumentException("Unsupported blobUrl: $blobUri")
      }

    return storageClient
  }

  companion object {
    fun parseBlobUri(url: String): BlobUri {
      val uri = URI.create(url)
      val bucket = uri.host
      val path = uri.path.removePrefix("/")
      return when (uri.scheme) {
        "s3" -> {
          throw IllegalArgumentException("S3 is not currently supported")
        }
        "gs" -> {
          BlobUri(scheme = "gs", bucket = bucket, path = path)
        }
        "file" -> {
          BlobUri(scheme = "file", bucket = null, path = path)
        }
        else -> throw IllegalArgumentException("Unable to parse blob uri $url")
      }
    }
  }
}

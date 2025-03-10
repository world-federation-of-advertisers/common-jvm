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
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import java.net.URI

// Data class to store parsed information
data class BlobUrl(
  val protocol: String,
  val bucket: String?,
  val region: String?,
  val project: String?,
  val path: String,
)

/*
 * Builds a [StorageClient] from a blob uri. Currently only supports Google Cloud Storage.
 * If file system storage should be supported, input a root File were data should be written/read.
 */
class StorageClientFactory(
  private val blobUrl: BlobUrl,
  private val file: File?,
) {

  constructor(url: String, file: File): this(parseBlobUrl(url)!!, file)

  fun build(): StorageClient {
    val storageClient: StorageClient =
      when (blobUrl.protocol) {
        "s3" -> {
          throw IllegalArgumentException("S3 is not currently supported")
        }
        "gs" -> {
          val storageOptions = StorageOptions.newBuilder().setProjectId(blobUrl.project!!).build()
          GcsStorageClient(storageOptions.service, blobUrl.bucket!!)
        }
        "file" -> {
          FileSystemStorageClient(directory = file!!)
        }
        else -> throw IllegalArgumentException("Unsupported blobUrl: $blobUrl")
      }

    return storageClient
  }

  companion object {
    fun parseBlobUrl(url: String): BlobUrl {
      val uri = URI.create(url)
      return when (uri.scheme) {
        "s3" -> {
          throw IllegalArgumentException("S3 is not currently supported")
        }
        "gs" -> {
          val bucket = uri.host
          val path = uri.path.removePrefix("/")
          val project = uri.query?.split("=")?.let { parts ->
            if (parts.size == 2 && parts[0] == "project") parts[1] else null
          }

          BlobUrl(
            protocol = "gs",
            bucket = bucket,
            region = null,
            project = project,
            path = path,
          )
        }
        "file" -> {
          BlobUrl(
            protocol = "file",
            bucket = null,
            region = null,
            project = null,
            path = uri.path,
          )
        }
        else -> throw IllegalArgumentException("Unable to parse blob uri $url")
      }
    }
  }
}

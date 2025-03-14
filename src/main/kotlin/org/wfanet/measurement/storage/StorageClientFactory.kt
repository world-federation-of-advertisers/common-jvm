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
data class BlobUri(val scheme: String, val bucket: String?, val region: String?, val path: String)

/*
 * Builds a [StorageClient] from a blob uri. Currently only supports Google Cloud Storage.
 * If file system storage should be supported, input a root File were data should be written/read.
 */
class StorageClientFactory(private val blobUrl: BlobUri, private val file: File?) {

  constructor(url: String, file: File) : this(parseBlobUri(url)!!, file)

  fun build(projectId: String? = null): StorageClient {
    val storageClient: StorageClient =
      when (blobUrl.scheme) {
        "s3" -> {
          throw IllegalArgumentException("S3 is not currently supported")
        }
        "gs" -> {
          val storageOptions =
            if (projectId != null) StorageOptions.newBuilder().setProjectId(projectId).build()
            else StorageOptions.getDefaultInstance()
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
    fun parseBlobUri(url: String): BlobUri {
      val uri = URI.create(url)
      return when (uri.scheme) {
        "s3" -> {
          throw IllegalArgumentException("S3 is not currently supported")
        }
        "gs" -> {
          val bucket = uri.host
          val path = uri.path.removePrefix("/")

          BlobUri(scheme = "gs", bucket = bucket, region = null, path = path)
        }
        "file" -> {
          BlobUri(scheme = "file", bucket = null, region = null, path = uri.path)
        }
        else -> throw IllegalArgumentException("Unable to parse blob uri $url")
      }
    }
  }
}

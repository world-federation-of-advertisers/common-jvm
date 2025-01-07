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
import java.util.regex.Pattern
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

// Data class to store parsed information
data class BlobUrl(
  val protocol: String,
  val bucket: String?,
  val region: String?,
  val project: String?,
  val path: String,
)

fun parseBlobUrl(url: String): BlobUrl? {
  // Define regex for different blob URL patterns
  val s3Regex =
    Pattern.compile("s3://(?<bucket>[^.]+)\\.s3\\.(?<region>[^.]+)\\.amazonaws\\.com/(?<path>.+)")
  val gsRegex =
    Pattern.compile("gs://(?<bucket>[^/]+)/(?<path>[^?]+)(?:\\?project=(?<project>[^&]+))?")
  val fileRegex = Pattern.compile("file://(?<path>.+)")
  // Match input URL with the regex
  val s3Matcher = s3Regex.matcher(url)
  if (s3Matcher.matches()) {
    throw IllegalArgumentException("S3 is not currently supported")
  }
  val gsMatcher = gsRegex.matcher(url)
  if (gsMatcher.matches()) {
    return BlobUrl(
      protocol = "gs",
      bucket = gsMatcher.group("bucket"),
      region = null,
      project = gsMatcher.group("project"),
      path = gsMatcher.group("path"),
    )
  }
  val fileMatcher = fileRegex.matcher(url)
  if (fileMatcher.matches()) {
    return BlobUrl(
      protocol = "file",
      bucket = null,
      region = null,
      project = null,
      path = fileMatcher.group("path"),
    )
  }
  return null
}

fun getStorageClient(blobUrl: BlobUrl): StorageClient {
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
        FileSystemStorageClient(directory = File("/"))
      }
      else -> throw IllegalArgumentException("Unsupported blobUrl: $blobUrl")
    }

  return storageClient
}

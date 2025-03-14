/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.storage

import com.google.common.truth.Truth.assertThat
import java.nio.file.Files
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class StorageClientFactoryTest {
  @Test
  fun testS3Url() {
    val s3Url = "s3://my-bucket.s3.us-west-2.amazonaws.com/path/to/file"
    assertThrows(IllegalArgumentException::class.java) { StorageClientFactory.parseBlobUri(s3Url) }
    val blobUrl = BlobUri("s3", "my-bucket", "us-west-2", "path/to/file")
    assertThrows(IllegalArgumentException::class.java) {
      StorageClientFactory(blobUrl, Files.createTempDirectory(null).toFile()).build()
    }
  }

  @Test
  fun testGsUrl() {
    val gcsUrl = "gs://my-bucket/path/to/file?project"
    val blobUrl = StorageClientFactory.parseBlobUri(gcsUrl)
    assertThat(blobUrl).isEqualTo(BlobUri("gs", "my-bucket", null, "path/to/file"))

    val storageClient =
      StorageClientFactory(blobUrl, Files.createTempDirectory(null).toFile()).build(projectId = "project-id")
    assertThat(storageClient is GcsStorageClient)
  }

  @Test
  fun testFileUrl() {
    val fileUrl = "file:///path/to/file"
    val blobUrl = StorageClientFactory.parseBlobUri(fileUrl)
    assertThat(blobUrl).isEqualTo(BlobUri("file", null, null, "/path/to/file"))

    val storageClient =
      StorageClientFactory(blobUrl, Files.createTempDirectory(null).toFile()).build()
    assertThat(storageClient is FileSystemStorageClient)
  }
}

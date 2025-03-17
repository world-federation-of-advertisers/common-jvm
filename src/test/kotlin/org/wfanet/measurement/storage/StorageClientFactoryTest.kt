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
  fun `parseBlobUri throws IllegalArgumentException when scheme is s3`() {
    val s3Url = "s3://my-bucket.s3.us-west-2.amazonaws.com/path/to/file"
    assertThrows(IllegalArgumentException::class.java) { StorageClientFactory.parseBlobUri(s3Url) }
    val blobUri = BlobUri("s3", "my-bucket", "path/to/file")
    assertThrows(IllegalArgumentException::class.java) {
      StorageClientFactory(blobUri, Files.createTempDirectory(null).toFile()).build()
    }
  }

  @Test
  fun `able to parse google cloud storage uri`() {
    val gcsUrl = "gs://my-bucket/path/to/file?project"
    val blobUri = StorageClientFactory.parseBlobUri(gcsUrl)
    assertThat(blobUri).isEqualTo(BlobUri("gs", "my-bucket", "path/to/file"))
  }

  @Test
  fun `gs uri returns google cloud storage client`() {
    val blobUri = BlobUri("gs", "my-bucket", "path/to/file")
    val storageClient =
      StorageClientFactory(blobUri, rootDirectory = null, projectId = "project-id").build()
    assertThat(storageClient is GcsStorageClient)
  }

  @Test
  fun `able to parse file system uri`() {
    val fileUrl = "file:///path/to/file"
    val blobUri = StorageClientFactory.parseBlobUri(fileUrl)
    assertThat(blobUri).isEqualTo(BlobUri("file", null, "path/to/file"))
  }

  @Test
  fun `file system uri returns file system storage client`() {
    val blobUri = BlobUri("file", null, "path/to/file")

    val storageClient =
      StorageClientFactory(blobUri, Files.createTempDirectory(null).toFile()).build()
    assertThat(storageClient is FileSystemStorageClient)
  }
}

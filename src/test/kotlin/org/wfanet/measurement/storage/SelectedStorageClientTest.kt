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
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.flatten

@RunWith(JUnit4::class)
class SelectedStorageClientTest {
  @Test
  fun `parseBlobUri throws IllegalArgumentException when scheme is s3`() {
    val s3Url = "s3://bucket.s3.us-west-2.amazonaws.com/path/to/file"
    assertThrows(IllegalArgumentException::class.java) { StorageClientFactory.parseBlobUri(s3Url) }
    val blobUri = BlobUri("s3", "bucket", "path/to/file")
    assertThrows(IllegalArgumentException::class.java) {
      StorageClientFactory(blobUri, Files.createTempDirectory(null).toFile()).build()
    }
  }

  @Test
  fun `able to parse google cloud storage uri`() {
    val gcsUrl = "gs://bucket/path/to/file?project"
    val blobUri = StorageClientFactory.parseBlobUri(gcsUrl)
    assertThat(blobUri).isEqualTo(BlobUri("gs", "bucket", "path/to/file"))
  }

  @Test
  fun `gs uri returns google cloud storage client`() {
    val blobUri = BlobUri("gs", "bucket", "path/to/file")
    val storageClient =
      StorageClientFactory(blobUri, rootDirectory = null, projectId = "project-id").build()
    assertThat(storageClient is GcsStorageClient)
  }

  @Test
  fun `able to parse file system uri`() {
    val fileUri = "file://bucket/path/to/file"
    val blobUri = StorageClientFactory.parseBlobUri(fileUri)
    assertThat(blobUri).isEqualTo(BlobUri("file", "bucket", "path/to/file"))
  }

  @Test
  fun `file system uri returns file system storage client`() {
    val blobUri = BlobUri("file", "bucket", "path/to/file")

    val storageClient =
      StorageClientFactory(blobUri, Files.createTempDirectory(null).toFile()).build()
    assertThat(storageClient is FileSystemStorageClient)
  }

  @Test
  fun `throws null if root directory is not present and scheme is file`() {
    val blobUri = BlobUri("file", "bucket", "path/to/file")

    assertThrows(IllegalStateException::class.java) {
      StorageClientFactory(blobUri).build()
    }
  }

  @Test
  fun `able to read blob from filesystem`() = runBlocking {
    val fileUri = "file://bucket/path/to/file"
    val content = flowOf( "a", "b", "c").map{ it.toByteStringUtf8() }
    val tmpPath = Files.createTempDirectory(null).toFile()
    StorageClientFactory.writeBlob(fileUri, content, tmpPath)
    val fileSystemStorageClient = FileSystemStorageClient(tmpPath)
    assertThat(fileSystemStorageClient.getBlob("bucket/path/to/file")!!.flatten()).isEqualTo(StorageClientFactory.getBlob())
  }

  @Test
  fun `able to read and write blob to filesystem`() {
    val fileUrl = "file://bucket/path/to/file"
    val content = flowOf( "a", "b", "c").map{ it.toByteStringUtf8() }
    StorageClientFactory.writeBlob
    assertThat(blobUri).isEqualTo(BlobUri("file", "bucket", "path/to/file"))
  }
}

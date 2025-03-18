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
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Files
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class SelectedStorageClientTest {
  @Test
  fun `s3 scheme throws IllegalArgumentException`() {
    val s3Url = "s3://bucket.s3.us-west-2.amazonaws.com/path/to/file"
    assertThrows(IllegalArgumentException::class.java) { SelectedStorageClient(s3Url) }
  }

  @Test
  fun `gs scheme returns client`() {
    val blobUri = BlobUri("gs", "bucket", "path/to/file")
    SelectedStorageClient(blobUri)
  }

  @Test
  fun `throws null if root directory is not present and scheme is file`() {
    val blobUri = BlobUri("file", "bucket", "path/to/file")

    assertThrows(IllegalStateException::class.java) { SelectedStorageClient(blobUri) }
  }

  @Test
  fun `able to write blob from filesystem`() = runBlocking {
    val fileUri = "file:///bucket/path/to"
    val content = flowOf("a", "b", "c").map { it.toByteStringUtf8() }
    val tmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(tmpPath.resolve("bucket/path/to").toPath())
    val client = SelectedStorageClient(fileUri, tmpPath)
    client.writeBlob("file", content)
    val fileSystemStorageClient = FileSystemStorageClient(tmpPath)
    assertThat(fileSystemStorageClient.getBlob("bucket/path/to/file")!!.read().flatten())
      .isEqualTo(content.flatten())
  }

  @Test
  fun `able to write and read blob to filesystem`() = runBlocking {
    val fileUri = "file:///bucket/path/to"
    val content = flowOf("a", "b", "c").map { it.toByteStringUtf8() }
    val tmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(tmpPath.resolve("bucket/path/to").toPath())
    val client = SelectedStorageClient(fileUri, tmpPath)
    client.writeBlob("file", content)
    val blob = client.getBlob("file")
    assertThat(blob!!.read().flatten()).isEqualTo(content.flatten())
  }

  @Test
  fun `able to write and read blob with full file path to filesystem`() = runBlocking {
    val fileUri = "file:///bucket/path/to/file"
    val content = flowOf("a", "b", "c").map { it.toByteStringUtf8() }
    val tmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(tmpPath.resolve("bucket/path/to").toPath())
    val client = SelectedStorageClient(fileUri, tmpPath)
    client.writeBlob("", content)
    val blob = client.getBlob("")
    assertThat(blob!!.read().flatten()).isEqualTo(content.flatten())
  }
}

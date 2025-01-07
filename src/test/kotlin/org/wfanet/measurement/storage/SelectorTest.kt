/*
 * Copyright 2021 The Cross-Media Measurement Authors
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
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class SelectorTest {
  @Test
  fun testS3Url() {
    val s3Url = "s3://my-bucket.s3.us-west-2.amazonaws.com/path/to/file"
    val blobUrl = parseBlobUrl(s3Url)!!
    assertThat(blobUrl).isEqualTo(BlobUrl("s3", "my-bucket", "us-west-2", null, "path/to/file"))

    val storageClient = getStorageClient(blobUrl)
    assertTrue(storageClient is S3StorageClient)
  }

  @Test
  fun testGsUrl() {
    val gsUrl = "gs://my-bucket/path/to/file?project=my-project"
    val blobUrl = parseBlobUrl(gsUrl)!!
    assertThat(blobUrl).isEqualTo(BlobUrl("gs", "my-bucket", null, "my-project", "path/to/file"))

    val storageClient = getStorageClient(blobUrl)
    assertThat(storageClient is GcsStorageClient)
  }

  @Test
  fun testFileUrl() {
    val fileUrl = "file:///path/to/file"
    val blobUrl = parseBlobUrl(fileUrl)!!
    assertThat(blobUrl).isEqualTo(BlobUrl("file", null, null, null, "/path/to/file"))

    val storageClient = getStorageClient(blobUrl)
    assertThat(storageClient is FileSystemStorageClient)
  }
}

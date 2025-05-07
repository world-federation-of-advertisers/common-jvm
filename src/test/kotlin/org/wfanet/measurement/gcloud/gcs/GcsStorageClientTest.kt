// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.gcs

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest

@RunWith(JUnit4::class)
class GcsStorageClientTest : AbstractStorageClientTest<GcsStorageClient>() {
  @Before
  fun initClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Test
  fun `listBlobNames without options gets all blob keys`() = runBlocking {
    prepareStorage()
    val blobKeys = storageClient.listBlobNames()
    assertThat(blobKeys).hasSize(3)
    assertThat(blobKeys.sorted()).isEqualTo(listOf(BLOB_KEY_1, BLOB_KEY_2, BLOB_KEY_3))
  }

  @Test
  fun `listBlobNames with delimiter options gets all first level file and folder names`() =
    runBlocking {
      prepareStorage()
      val blobKeys = storageClient.listBlobNames(delimiter = "/")
      assertThat(blobKeys).hasSize(3)
      assertThat(blobKeys.sorted()).isEqualTo(listOf("dir1/", "dir2/", "file3.textproto"))
    }

  @Test
  fun `listBlobNames with prefix options gets blob keys that match the prefix`() = runBlocking {
    prepareStorage()
    val blobKeys = storageClient.listBlobNames(prefix = "dir1")
    assertThat(blobKeys).hasSize(1)
    assertThat(blobKeys).isEqualTo(listOf(BLOB_KEY_1))
  }

  @Test
  fun `listBlobNames with both options list names directly under the directory containing the prefix`() =
    runBlocking {
      prepareStorage()
      val blobKeys1 = storageClient.listBlobNames(prefix = "dir", delimiter = "/")
      assertThat(blobKeys1).hasSize(2)
      assertThat(blobKeys1.sorted()).isEqualTo(listOf("dir1/", "dir2/"))

      val blobKeys2 = storageClient.listBlobNames(prefix = "dir1/", delimiter = "/")
      assertThat(blobKeys2).hasSize(1)
      assertThat(blobKeys2).isEqualTo(listOf("dir1/file1.textproto"))
    }

  private fun prepareStorage() {
    runBlocking {
      storageClient.writeBlob(BLOB_KEY_1, "content1".toByteStringUtf8())
      storageClient.writeBlob(BLOB_KEY_2, "content2".toByteStringUtf8())
      storageClient.writeBlob(BLOB_KEY_3, "content3".toByteStringUtf8())
    }
  }

  companion object {
    private const val BUCKET = "test-bucket"
    private const val BLOB_KEY_1 = "dir1/file1.textproto"
    private const val BLOB_KEY_2 = "dir2/file2.textproto"
    private const val BLOB_KEY_3 = "file3.textproto"
  }
}

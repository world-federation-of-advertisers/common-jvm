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

package org.wfanet.measurement.storage.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.random.Random
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.size
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

/** Abstract base class for testing implementations of [StorageClient]. */
abstract class AbstractStorageClientTest<T : StorageClient> {
  protected val testBlobContent: ByteString
    get() = Companion.testBlobContent

  open fun computeStoredBlobSize(content: ByteString, blobKey: String): Int {
    return content.size
  }

  protected lateinit var storageClient: T

  @Test
  fun `writeBlob returns new readable blob`() = runBlocking {
    val blobKey = "new-blob"

    val blob = storageClient.writeBlob(blobKey, testBlobContent)

    assertThat(blob).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlob overwrites existing blob`() = runBlocking {
    val blobKey = "new-blob"
    storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())

    val blob = storageClient.writeBlob(blobKey, testBlobContent)

    assertThat(blob).contentEqualTo(testBlobContent)
  }

  @Test
  fun `getBlob returns null for non-existant blob`() = runBlocking {
    val blobKey = "non-existant-blob"
    assertThat(storageClient.getBlob(blobKey)).isNull()
  }

  @Test
  fun `getBlob returns readable Blob`() = runBlocking {
    val blobKey = "blob-to-get"
    storageClient.writeBlob(blobKey, testBlobContent)

    val blob = assertNotNull(storageClient.getBlob(blobKey))

    assertThat(blob).contentEqualTo(testBlobContent)
  }

  @Test
  fun `Blob size returns content size`() = runBlocking {
    val blobKey = "blob-to-check-size-" + random.nextInt().toString()

    val blob = storageClient.writeBlob(blobKey, testBlobContent)

    assertThat(blob).hasSize(computeStoredBlobSize(testBlobContent, blobKey))
  }

  @Test
  fun `Blob delete deletes blob`() = runBlocking {
    val blobKey = "blob-to-delete"
    val blob = storageClient.writeBlob(blobKey, testBlobContent)

    blob.delete()

    assertThat(storageClient.getBlob(blobKey)).isNull()
  }

  @Test
  fun `Write and read empty blob`() = runBlocking {
    val blobKey = "empty-blob"
    storageClient.writeBlob(blobKey, emptyFlow())
    val blob = assertNotNull(storageClient.getBlob(blobKey))

    assertThat(blob).contentEqualTo(ByteString.EMPTY)
  }

  @Test
  fun `listBlobs with prefix gets blobs with blob keys that match the prefix`() = runBlocking {
    prepareStorage()
    val blobs = storageClient.listBlobs(prefix = "dir1").toList()
    assertThat(blobs).hasSize(1)
    assertThat(blobs.first().blobKey).isEqualTo(BLOB_KEY_1)
  }

  @Test
  fun `listBlobs with prefix does not match middle of blob key`() = runBlocking {
    prepareStorage()
    val blobs = storageClient.listBlobs(prefix = "file2").toList()
    assertThat(blobs).hasSize(0)
  }

  @Test
  fun `listBlobs with empty prefix gets all blobs`() = runBlocking {
    prepareStorage()
    val blobs = storageClient.listBlobs(prefix = "").toList()
    assertThat(blobs).hasSize(3)
  }

  @Test
  fun `listBlobs with no arguments gets all blobs`() = runBlocking {
    prepareStorage()
    val blobs = storageClient.listBlobs().toList()
    assertThat(blobs).hasSize(3)
  }

  private fun prepareStorage() {
    runBlocking {
      storageClient.writeBlob(BLOB_KEY_1, "content1".toByteStringUtf8())
      storageClient.writeBlob(BLOB_KEY_2, "content2".toByteStringUtf8())
      storageClient.writeBlob(BLOB_KEY_3, "content3".toByteStringUtf8())
    }
  }

  companion object {
    private val random = Random.Default
    private val testBlobContent: ByteString =
      random.nextBytes(random.nextInt(BYTES_PER_MIB * 3, BYTES_PER_MIB * 4)).toByteString()

    private const val BLOB_KEY_1 = "dir1/file1.textproto"
    private const val BLOB_KEY_2 = "dir2/file2.textproto"
    private const val BLOB_KEY_3 = "file3.textproto"
  }
}

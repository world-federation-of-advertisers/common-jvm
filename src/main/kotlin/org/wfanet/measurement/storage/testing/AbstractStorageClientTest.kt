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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.size
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat
import org.wfanet.measurement.storage.writeBlob

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
  fun `getBlob returns null for non-existant blob`() {
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

  companion object {
    private val random = Random.Default
    private val testBlobContent: ByteString =
      random.nextBytes(random.nextInt(BYTES_PER_MIB * 3, BYTES_PER_MIB * 4)).toByteString()
  }
}

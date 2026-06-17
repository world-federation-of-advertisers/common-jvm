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

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule
import org.wfanet.measurement.storage.testing.AbstractBlobMetadataStorageClientTest
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

@RunWith(JUnit4::class)
class GcsStorageClientTest : AbstractBlobMetadataStorageClientTest<GcsStorageClient>() {
  @Before
  fun initClient() {
    storageEmulator.createBucket(BUCKET)
    storageClient = GcsStorageClient(storageEmulator.storage, BUCKET)
  }

  override suspend fun getGeneration(blobKey: String): Long =
    storageEmulator.storage.get(BUCKET, blobKey).generation

  @After
  fun deleteBucket() {
    storageEmulator.deleteBucketRecursive(BUCKET)
  }

  override suspend fun verifyBlobMetadata(
    blobKey: String,
    expectedCustomCreateTime: Instant?,
    expectedMetadata: Map<String, String>,
  ) {
    val blob = storageEmulator.storage.get(BUCKET, blobKey)
    checkNotNull(blob) { "Blob not found: $blobKey" }

    if (expectedCustomCreateTime != null) {
      val actualCustomTime = blob.customTimeOffsetDateTime
      checkNotNull(actualCustomTime) { "Custom time not set on blob: $blobKey" }
      assertThat(actualCustomTime.toInstant()).isEqualTo(expectedCustomCreateTime)
    }

    if (expectedMetadata.isNotEmpty()) {
      val actualMetadata = blob.metadata ?: emptyMap()
      assertThat(actualMetadata).containsAtLeastEntriesIn(expectedMetadata)
    }
  }

  /**
   * GCS-specific test: a metadata PATCH that sets a key to `null` deletes that key from the stored
   * object. This documents the SDK behavior our [GcsStorageClient.ClientBlob.metadata] getter
   * relies on (see the KDoc on that property) — fetched objects never carry null-valued metadata
   * entries, so the defensive null-filter in the getter is a no-op against real (and emulated) GCS.
   *
   * Lives in this class rather than the abstract conformance suite because the "PATCH key=null
   * deletes the key" behavior is a GCS REST/SDK quirk, not a property of the [StorageClient]
   * interface.
   */
  @Test
  fun `PATCH with null metadata value deletes the key on GCS`(): Unit = runBlocking {
    val blobKey = "null-meta-blob"
    storageClient.writeBlob(blobKey, "x".toByteStringUtf8())
    storageClient.updateBlobMetadata(blobKey, metadata = mapOf("foo" to "bar"))

    // Issue a low-level PATCH that maps "foo" -> null via the underlying GCS SDK.
    val patch =
      BlobInfo.newBuilder(BlobId.of(BUCKET, blobKey))
        .apply { setMetadata(mapOf("foo" to null)) }
        .build()
    storageEmulator.storage.update(patch)

    val refetched = checkNotNull(storageClient.getBlob(blobKey))
    assertThat(refetched.metadata).doesNotContainKey("foo")
  }

  companion object {
    private const val BUCKET = "test-bucket"

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()
  }
}

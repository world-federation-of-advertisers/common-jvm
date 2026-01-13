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

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import java.time.ZoneOffset
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule
import org.wfanet.measurement.storage.testing.AbstractObjectMetadataStorageClientTest

@RunWith(JUnit4::class)
class GcsStorageClientTest : AbstractObjectMetadataStorageClientTest<GcsStorageClient>() {
  @Before
  fun initClient() {
    storageEmulator.createBucket(BUCKET)
    storageClient = GcsStorageClient(storageEmulator.storage, BUCKET)
  }

  @After
  fun deleteBucket() {
    storageEmulator.deleteBucketRecursive(BUCKET)
  }

  override suspend fun verifyObjectMetadata(
    blobKey: String,
    expectedCustomTime: Instant?,
    expectedMetadata: Map<String, String>,
  ) {
    val blob = storageEmulator.storage.get(BUCKET, blobKey)
    checkNotNull(blob) { "Blob not found: $blobKey" }

    if (expectedCustomTime != null) {
      val actualCustomTime = blob.customTimeOffsetDateTime
      checkNotNull(actualCustomTime) { "Custom time not set on blob: $blobKey" }
      assertThat(actualCustomTime.toInstant()).isEqualTo(expectedCustomTime)
    }

    if (expectedMetadata.isNotEmpty()) {
      val actualMetadata = blob.metadata ?: emptyMap()
      assertThat(actualMetadata).containsAtLeastEntriesIn(expectedMetadata)
    }
  }

  companion object {
    private const val BUCKET = "test-bucket"

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()
  }
}

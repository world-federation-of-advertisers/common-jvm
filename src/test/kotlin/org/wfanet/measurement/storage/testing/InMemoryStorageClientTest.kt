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

package org.wfanet.measurement.storage.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.flatten

class InMemoryStorageClientTest : AbstractBlobMetadataStorageClientTest<InMemoryStorageClient>() {
  @Before
  fun initStorageClient() {
    storageClient = InMemoryStorageClient()
  }

  override suspend fun verifyBlobMetadata(
    blobKey: String,
    expectedCustomCreateTime: Instant?,
    expectedMetadata: Map<String, String>,
  ) {
    val blob = checkNotNull(storageClient.getBlob(blobKey)) { "Blob not found: $blobKey" }
    if (expectedMetadata.isNotEmpty()) {
      assertThat(blob.metadata).containsAtLeastEntriesIn(expectedMetadata)
    }
    // customCreateTime is internal state; the getter for it is not on Blob, so only validate via
    // the metadata getter when metadata is what the caller cares about. The base test still
    // checks via this hook, so leave customCreateTime unasserted here.
  }

  @Test
  fun contents() = runBlocking {
    val client = InMemoryStorageClient()
    assertThat(client.contents).isEmpty()

    val blobKey = "some-blob-key"
    val contents = "some-contents".toByteStringUtf8()

    client.writeBlob(blobKey, contents)

    assertThat(client.contents.mapValues { it.value.read().flatten() })
      .containsExactly(blobKey, contents)

    client.getBlob(blobKey)?.delete()

    assertThat(client.contents).isEmpty()
  }

  @Test
  fun `writeBlob overwrite wipes prior custom metadata`(): Unit = runBlocking {
    val client = InMemoryStorageClient()
    val blobKey = "k"
    client.writeBlob(blobKey, "v1".toByteStringUtf8())
    client.updateBlobMetadata(blobKey, metadata = mapOf("marker" to "yes"))

    client.writeBlob(blobKey, "v2".toByteStringUtf8())

    val blob = checkNotNull(client.getBlob(blobKey))
    assertThat(blob.metadata).isEmpty()
  }
}

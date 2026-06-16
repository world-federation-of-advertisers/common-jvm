/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.storage.testing

import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

abstract class AbstractConditionalOperationStorageClientTest<
  T : ConditionalOperationStorageClient
> : AbstractStorageClientTest<T>() {

  @Test
  fun `writeBlobIfUnchanged overwrites existing blob`(): Unit = runBlocking {
    val blobKey = "replacable-blob"
    val blob = storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())

    val replacedBlob = storageClient.writeBlobIfUnchanged(blob, flowOf(testBlobContent))

    assertThat(replacedBlob).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfUnchanged throws error when blob contents changed`(): Unit = runBlocking {
    val blobKey = "blob"
    val blob = storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())
    storageClient.writeBlob(blobKey, "other content".toByteStringUtf8())

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfUnchanged(blob, flowOf(testBlobContent))
    }
  }

  @Test
  fun `writeBlobIfAbsent writes when no blob exists`(): Unit = runBlocking {
    val blobKey = "fresh-blob"

    val written = storageClient.writeBlobIfAbsent(blobKey, flowOf(testBlobContent))

    assertThat(written).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfAbsent throws BlobChangedException when blob exists`(): Unit = runBlocking {
    val blobKey = "existing-blob"
    storageClient.writeBlob(blobKey, "first writer".toByteStringUtf8())

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfAbsent(blobKey, flowOf(testBlobContent))
    }

    assertThat(checkNotNull(storageClient.getBlob(blobKey)))
      .contentEqualTo("first writer".toByteStringUtf8())
  }

  @Test
  fun `writeBlobIfAbsent leaves no blob when precondition fails`(): Unit = runBlocking {
    // Regression test: a failed `writeBlobIfAbsent` must not somehow create or partially-write a
    // new blob. The blob seen after the throw must still be byte-identical to what was there
    // before.
    val blobKey = "regression-blob"
    val originalContent = "first writer wins".toByteStringUtf8()
    storageClient.writeBlob(blobKey, originalContent)

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfAbsent(blobKey, flowOf(testBlobContent))
    }

    val after = checkNotNull(storageClient.getBlob(blobKey))
    assertThat(after).contentEqualTo(originalContent)
  }

  @Test
  fun `writeBlobIfAbsent succeeds with empty content`(): Unit = runBlocking {
    val blobKey = "empty-blob"

    val written = storageClient.writeBlobIfAbsent(blobKey, emptyFlow())

    assertThat(written).contentEqualTo("".toByteStringUtf8())
  }

  @Test
  fun `writeBlobIfAbsent then writeBlob overwrites`(): Unit = runBlocking {
    // Once a blob exists, an unconditional writeBlob should still overwrite it —
    // `writeBlobIfAbsent`
    // does not lock the key against future unconditional writes.
    val blobKey = "overwritable-blob"
    storageClient.writeBlobIfAbsent(blobKey, "first".toByteStringUtf8().let { flowOf(it) })

    val overwritten = storageClient.writeBlob(blobKey, "second".toByteStringUtf8())

    assertThat(overwritten).contentEqualTo("second".toByteStringUtf8())
  }
}

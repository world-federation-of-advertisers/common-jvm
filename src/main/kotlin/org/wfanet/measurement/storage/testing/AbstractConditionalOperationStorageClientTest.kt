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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

abstract class AbstractConditionalOperationStorageClientTest<
  T : ConditionalOperationStorageClient
> : AbstractStorageClientTest<T>() {

  /**
   * Returns the storage-backend generation for the blob at [blobKey], or throws if the blob does
   * not exist. Each backend exposes generation differently, so the conformance suite delegates to
   * the test subclass.
   */
  protected abstract suspend fun getGeneration(blobKey: String): Long

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
  fun `writeBlobIfGeneration with expected 0 writes when no blob exists`(): Unit = runBlocking {
    val blobKey = "fresh-blob"

    val written =
      storageClient.writeBlobIfGeneration(blobKey, expectedGeneration = 0L, flowOf(testBlobContent))

    assertThat(written).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfGeneration with expected 0 throws when blob exists`(): Unit = runBlocking {
    val blobKey = "existing-blob"
    storageClient.writeBlob(blobKey, "first writer".toByteStringUtf8())

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfGeneration(blobKey, expectedGeneration = 0L, flowOf(testBlobContent))
    }

    assertThat(checkNotNull(storageClient.getBlob(blobKey)))
      .contentEqualTo("first writer".toByteStringUtf8())
  }

  @Test
  fun `writeBlobIfGeneration leaves blob untouched when precondition fails`(): Unit = runBlocking {
    val blobKey = "regression-blob"
    val originalContent = "first writer wins".toByteStringUtf8()
    storageClient.writeBlob(blobKey, originalContent)

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfGeneration(blobKey, expectedGeneration = 0L, flowOf(testBlobContent))
    }

    val after = checkNotNull(storageClient.getBlob(blobKey))
    assertThat(after).contentEqualTo(originalContent)
  }

  @Test
  fun `writeBlobIfGeneration with expected 0 succeeds with empty content`(): Unit = runBlocking {
    val blobKey = "empty-blob"

    val written = storageClient.writeBlobIfGeneration(blobKey, expectedGeneration = 0L, emptyFlow())

    assertThat(written).contentEqualTo("".toByteStringUtf8())
  }

  @Test
  fun `writeBlobIfGeneration does not lock the key against later unconditional writes`(): Unit =
    runBlocking {
      val blobKey = "overwritable-blob"
      storageClient.writeBlobIfGeneration(
        blobKey,
        expectedGeneration = 0L,
        flowOf("first".toByteStringUtf8()),
      )

      val overwritten = storageClient.writeBlob(blobKey, "second".toByteStringUtf8())

      assertThat(overwritten).contentEqualTo("second".toByteStringUtf8())
    }

  @Test
  fun `writeBlobIfGeneration with stale generation throws`(): Unit = runBlocking {
    val blobKey = "stale-gen-cas"
    storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
    val staleGen = getGeneration(blobKey)
    // Concurrent writer races ahead.
    storageClient.writeBlob(blobKey, "v1.5".toByteStringUtf8())

    // Sanity check: the two writes must produce distinct generations or the assertion below
    // would pass for the wrong reason (no precondition violation). Backends with coarse
    // generation resolution (e.g. filesystem `lastModified` at millisecond precision) must
    // ensure monotonic advancement; this asserts they do.
    assertThat(getGeneration(blobKey)).isNotEqualTo(staleGen)

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfGeneration(blobKey, staleGen, flowOf(testBlobContent))
    }
  }

  @Test
  fun `writeBlobIfGeneration with matching generation overwrites`(): Unit = runBlocking {
    val blobKey = "matching-gen-cas"
    storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
    val currentGen = getGeneration(blobKey)

    val second = storageClient.writeBlobIfGeneration(blobKey, currentGen, flowOf(testBlobContent))

    assertThat(second).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfGeneration rejects negative expectedGeneration`(): Unit = runBlocking {
    storageClient.writeBlob("k", "v".toByteStringUtf8())

    assertFailsWith<IllegalArgumentException> {
      storageClient.writeBlobIfGeneration("k", expectedGeneration = -1L, flowOf(testBlobContent))
    }
  }

  @Test
  fun `writeBlobIfGeneration rejects negative expectedGeneration before checking storage`(): Unit =
    runBlocking {
      // Distinct from the test above: this one targets a nonexistent key so that any
      // implementation that did `getBlob`-then-validate (instead of validating first) would
      // throw `BlobChangedException` or `StorageException` rather than `IllegalArgumentException`.
      // Verifies the guard is at the call boundary.
      assertFailsWith<IllegalArgumentException> {
        storageClient.writeBlobIfGeneration(
          "nonexistent-key",
          expectedGeneration = -1L,
          flowOf(testBlobContent),
        )
      }
    }

  @Test
  fun `listBlobs returns all blobs without duplicates`(): Unit = runBlocking {
    val totalBlobs = 25
    val prefix = "test-listblobs/"
    for (i in 0 until totalBlobs) {
      storageClient.writeBlob("${prefix}blob-$i", "content-$i".toByteStringUtf8())
    }

    val keys = storageClient.listBlobs(prefix).toList().map { it.blobKey }

    assertThat(keys).hasSize(totalBlobs)
    assertThat(keys.toSet()).hasSize(totalBlobs)
  }
}

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

  @Test
  fun `writeBlobIfUnchanged(blob) overwrites existing blob`(): Unit = runBlocking {
    val blobKey = "replacable-blob"
    val blob = storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())

    val replacedBlob = storageClient.writeBlobIfUnchanged(blob, flowOf(testBlobContent))

    assertThat(replacedBlob).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfUnchanged(blob) throws error when blob contents changed`(): Unit = runBlocking {
    val blobKey = "blob"
    val blob = storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())
    storageClient.writeBlob(blobKey, "other content".toByteStringUtf8())

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfUnchanged(blob, flowOf(testBlobContent))
    }
  }

  @Test
  fun `getFreshnessToken returns null for nonexistent blob`(): Unit = runBlocking {
    assertThat(storageClient.getFreshnessToken("nonexistent")).isNull()
  }

  @Test
  fun `getFreshnessToken returns non-null token for existing blob`(): Unit = runBlocking {
    val blobKey = "existing"
    storageClient.writeBlob(blobKey, testBlobContent)

    assertThat(storageClient.getFreshnessToken(blobKey)).isNotNull()
  }

  @Test
  fun `getFreshnessToken token changes across writes`(): Unit = runBlocking {
    val blobKey = "rewritten"
    storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
    val first = checkNotNull(storageClient.getFreshnessToken(blobKey))
    storageClient.writeBlob(blobKey, "v2".toByteStringUtf8())
    val second = checkNotNull(storageClient.getFreshnessToken(blobKey))

    assertThat(second).isNotEqualTo(first)
  }

  @Test
  fun `writeBlobIfUnchanged(token) overwrites when token matches`(): Unit = runBlocking {
    val blobKey = "matching-token-cas"
    storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
    val token = checkNotNull(storageClient.getFreshnessToken(blobKey))

    val written = storageClient.writeBlobIfUnchanged(blobKey, token, flowOf(testBlobContent))

    assertThat(written).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfUnchanged(token) throws when token stale`(): Unit = runBlocking {
    val blobKey = "stale-token-cas"
    storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
    val staleToken = checkNotNull(storageClient.getFreshnessToken(blobKey))
    // Concurrent writer races ahead.
    storageClient.writeBlob(blobKey, "v1.5".toByteStringUtf8())

    // Sanity check: the two writes must produce distinct tokens or the assertion below would
    // pass for the wrong reason. Backends with coarse generation resolution (e.g. filesystem
    // `lastModified` at millisecond precision) must ensure monotonic advancement; this asserts
    // they do.
    assertThat(checkNotNull(storageClient.getFreshnessToken(blobKey))).isNotEqualTo(staleToken)

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfUnchanged(blobKey, staleToken, flowOf(testBlobContent))
    }
  }

  @Test
  fun `writeBlobIfUnchanged(token) throws when blob no longer exists`(): Unit = runBlocking {
    val blobKey = "vanished-cas"
    val blob = storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
    val token = checkNotNull(storageClient.getFreshnessToken(blobKey))
    blob.delete()

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfUnchanged(blobKey, token, flowOf(testBlobContent))
    }
  }

  @Test
  fun `writeBlobIfUnchanged(token) leaves blob untouched when precondition fails`(): Unit =
    runBlocking {
      val blobKey = "regression-cas"
      storageClient.writeBlob(blobKey, "v1".toByteStringUtf8())
      val staleToken = checkNotNull(storageClient.getFreshnessToken(blobKey))
      // Concurrent writer races ahead.
      val winnerContent = "v1.5".toByteStringUtf8()
      storageClient.writeBlob(blobKey, winnerContent)

      assertFailsWith<BlobChangedException> {
        storageClient.writeBlobIfUnchanged(blobKey, staleToken, flowOf(testBlobContent))
      }

      // The contract is "the failed write didn't apply" — the winner's bytes remain, not the
      // original v1. This catches an impl that mutated before checking the precondition.
      assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(winnerContent)
    }

  @Test
  fun `getFreshnessToken result composes with writeBlobIfUnchanged(token)`(): Unit = runBlocking {
    // Models the canonical workflow: capture the token at workflow start, then CAS-write
    // later using only the captured token (no live Blob reference). Proves the conditional
    // surface composes end-to-end on every backend.
    val blobKey = "workflow-roundtrip"
    storageClient.writeBlob(blobKey, "initial".toByteStringUtf8())

    val token = checkNotNull(storageClient.getFreshnessToken(blobKey))
    // ... time passes; the Blob reference is gone, only the token survives ...

    val written = storageClient.writeBlobIfUnchanged(blobKey, token, flowOf(testBlobContent))

    assertThat(written).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `null token path then unchanged-token path composes correctly`(): Unit = runBlocking {
    // Models the full two-dispatch workflow: first time through, the blob doesn't exist and
    // the caller writes via writeBlobIfNotFound; second time through, the blob exists and the
    // caller captures the token and CASes the new content in. This is exactly the if/else the
    // TEE app uses around getFreshnessToken's null/non-null result.
    val blobKey = "two-pass-workflow"

    // Pass 1: blob does not exist.
    assertThat(storageClient.getFreshnessToken(blobKey)).isNull()
    storageClient.writeBlobIfNotFound(blobKey, flowOf("v1".toByteStringUtf8()))

    // Pass 2: blob exists; capture token, CAS-write.
    val token = checkNotNull(storageClient.getFreshnessToken(blobKey))
    storageClient.writeBlobIfUnchanged(blobKey, token, flowOf(testBlobContent))

    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfNotFound writes when no blob exists`(): Unit = runBlocking {
    val blobKey = "fresh-blob"

    val written = storageClient.writeBlobIfNotFound(blobKey, flowOf(testBlobContent))

    assertThat(written).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfNotFound throws when blob exists`(): Unit = runBlocking {
    val blobKey = "existing-blob"
    storageClient.writeBlob(blobKey, "first writer".toByteStringUtf8())

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfNotFound(blobKey, flowOf(testBlobContent))
    }

    assertThat(checkNotNull(storageClient.getBlob(blobKey)))
      .contentEqualTo("first writer".toByteStringUtf8())
  }

  @Test
  fun `writeBlobIfNotFound leaves blob untouched when precondition fails`(): Unit = runBlocking {
    val blobKey = "regression-blob"
    val originalContent = "first writer wins".toByteStringUtf8()
    storageClient.writeBlob(blobKey, originalContent)

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfNotFound(blobKey, flowOf(testBlobContent))
    }

    val after = checkNotNull(storageClient.getBlob(blobKey))
    assertThat(after).contentEqualTo(originalContent)
  }

  @Test
  fun `writeBlobIfNotFound succeeds with empty content`(): Unit = runBlocking {
    val blobKey = "empty-blob"

    val written = storageClient.writeBlobIfNotFound(blobKey, emptyFlow())

    assertThat(written).contentEqualTo("".toByteStringUtf8())
  }

  @Test
  fun `writeBlobIfNotFound does not lock the key against later unconditional writes`(): Unit =
    runBlocking {
      val blobKey = "overwritable-blob"
      storageClient.writeBlobIfNotFound(blobKey, flowOf("first".toByteStringUtf8()))

      val overwritten = storageClient.writeBlob(blobKey, "second".toByteStringUtf8())

      assertThat(overwritten).contentEqualTo("second".toByteStringUtf8())
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

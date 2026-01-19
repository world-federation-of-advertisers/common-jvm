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
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageException

/**
 * Abstract base class for testing implementations of [BlobMetadataStorageClient].
 *
 * Subclasses must implement [verifyBlobMetadata] to verify that metadata was correctly set on the
 * storage backend.
 */
abstract class AbstractBlobMetadataStorageClientTest<T> :
  AbstractConditionalOperationStorageClientTest<T>() where
T : BlobMetadataStorageClient,
T : ConditionalOperationStorageClient {

  /**
   * Verifies that the blob metadata was set correctly.
   *
   * @param blobKey the key of the blob to verify
   * @param expectedCustomCreateTime the expected custom create time, or null if not set
   * @param expectedMetadata the expected custom metadata
   */
  protected abstract suspend fun verifyBlobMetadata(
    blobKey: String,
    expectedCustomCreateTime: Instant?,
    expectedMetadata: Map<String, String>,
  )

  @Test
  fun `updateBlobMetadata sets custom create time on existing blob`(): Unit = runBlocking {
    val blobKey = "blob-with-custom-time"
    storageClient.writeBlob(blobKey, "test content".toByteStringUtf8())
    val customCreateTime = Instant.parse("2025-01-15T10:30:00Z")

    storageClient.updateBlobMetadata(blobKey, customCreateTime = customCreateTime)

    verifyBlobMetadata(
      blobKey,
      expectedCustomCreateTime = customCreateTime,
      expectedMetadata = emptyMap(),
    )
  }

  @Test
  fun `updateBlobMetadata sets custom metadata on existing blob`(): Unit = runBlocking {
    val blobKey = "blob-with-metadata"
    storageClient.writeBlob(blobKey, "test content".toByteStringUtf8())
    val metadata = mapOf("key1" to "value1", "key2" to "value2")

    storageClient.updateBlobMetadata(blobKey, metadata = metadata)

    verifyBlobMetadata(blobKey, expectedCustomCreateTime = null, expectedMetadata = metadata)
  }

  @Test
  fun `updateBlobMetadata sets both custom create time and metadata`(): Unit = runBlocking {
    val blobKey = "blob-with-both"
    storageClient.writeBlob(blobKey, "test content".toByteStringUtf8())
    val customCreateTime = Instant.parse("2025-01-15T10:30:00Z")
    val metadata = mapOf("resource-id" to "dataProviders/abc/impressionMetadata/xyz")

    storageClient.updateBlobMetadata(
      blobKey,
      customCreateTime = customCreateTime,
      metadata = metadata,
    )

    verifyBlobMetadata(
      blobKey,
      expectedCustomCreateTime = customCreateTime,
      expectedMetadata = metadata,
    )
  }

  @Test
  fun `updateBlobMetadata throws exception for non-existent blob`(): Unit = runBlocking {
    val blobKey = "non-existent-blob"
    val customCreateTime = Instant.parse("2025-01-15T10:30:00Z")

    assertFailsWith<StorageException> {
      storageClient.updateBlobMetadata(blobKey, customCreateTime = customCreateTime)
    }
  }
}

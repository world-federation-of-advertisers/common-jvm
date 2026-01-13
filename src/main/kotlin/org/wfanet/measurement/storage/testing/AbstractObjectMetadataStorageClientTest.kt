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
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ObjectMetadataStorageClient
import org.wfanet.measurement.storage.StorageException

/**
 * Abstract base class for testing implementations of [ObjectMetadataStorageClient].
 *
 * Subclasses must implement [verifyObjectMetadata] to verify that metadata was correctly set on the
 * storage backend.
 */
abstract class AbstractObjectMetadataStorageClientTest<T> :
  AbstractConditionalOperationStorageClientTest<T>()
  where T : ObjectMetadataStorageClient, T : ConditionalOperationStorageClient {

  /**
   * Verifies that the object metadata was set correctly.
   *
   * @param blobKey the key of the blob to verify
   * @param expectedCustomTime the expected custom time, or null if not set
   * @param expectedMetadata the expected custom metadata
   */
  protected abstract suspend fun verifyObjectMetadata(
    blobKey: String,
    expectedCustomTime: Instant?,
    expectedMetadata: Map<String, String>,
  )

  @Test
  fun `updateObjectMetadata sets custom time on existing blob`(): Unit = runBlocking {
    val blobKey = "blob-with-custom-time"
    storageClient.writeBlob(blobKey, "test content".toByteStringUtf8())
    val customTime = Instant.parse("2025-01-15T10:30:00Z")

    storageClient.updateObjectMetadata(blobKey, customTime = customTime)

    verifyObjectMetadata(blobKey, expectedCustomTime = customTime, expectedMetadata = emptyMap())
  }

  @Test
  fun `updateObjectMetadata sets custom metadata on existing blob`(): Unit = runBlocking {
    val blobKey = "blob-with-metadata"
    storageClient.writeBlob(blobKey, "test content".toByteStringUtf8())
    val metadata = mapOf("key1" to "value1", "key2" to "value2")

    storageClient.updateObjectMetadata(blobKey, metadata = metadata)

    verifyObjectMetadata(blobKey, expectedCustomTime = null, expectedMetadata = metadata)
  }

  @Test
  fun `updateObjectMetadata sets both custom time and metadata`(): Unit = runBlocking {
    val blobKey = "blob-with-both"
    storageClient.writeBlob(blobKey, "test content".toByteStringUtf8())
    val customTime = Instant.parse("2025-01-15T10:30:00Z")
    val metadata = mapOf("resource-id" to "dataProviders/abc/impressionMetadata/xyz")

    storageClient.updateObjectMetadata(blobKey, customTime = customTime, metadata = metadata)

    verifyObjectMetadata(blobKey, expectedCustomTime = customTime, expectedMetadata = metadata)
  }

  @Test
  fun `updateObjectMetadata throws exception for non-existent blob`(): Unit = runBlocking {
    val blobKey = "non-existent-blob"
    val customTime = Instant.parse("2025-01-15T10:30:00Z")

    assertFailsWith<StorageException> {
      storageClient.updateObjectMetadata(blobKey, customTime = customTime)
    }
  }
}


/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Bare-bones [StorageClient] that does NOT implement [ConditionalOperationStorageClient].
 *
 * Used by wrapper-client tests (e.g. `AeadStorageClientTest`, `MesosRecordIoStorageClientTest`) to
 * verify that constructing a wrapper around a non-conditional underlying client throws
 * [IllegalArgumentException] at construction.
 */
class NonConditionalStorageClient : StorageClient {
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
    error("not used")

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? = null

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> = flowOf()
}

// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.storage

import com.google.common.truth.Truth
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

class ShardedStorageClientTest {
  private lateinit var wrappedStorageClient: StorageClient
  private lateinit var shardedtorageClient: ShardedStorageClient

  @Before
  fun initStorageClient() {
    wrappedStorageClient = InMemoryStorageClient()
    shardedtorageClient = ShardedStorageClient(MesosRecordIoStorageClient(wrappedStorageClient))
  }

  @Test
  fun `test writing and reading single record`() = runBlocking {
    val testData = "impression1"
    val blobKey = "/labelled-impressions/ds/2025-02-14/event-group-id/12345/sharded-impressions"
    val testDataFlow = flowOf(ByteString.copyFromUtf8(testData))
    shardedtorageClient.writeBlob(blobKey, testDataFlow, 2)
    val blob = shardedtorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val results = blob.read().toList()
    Truth.assertThat(1).isEqualTo(results.size)
    Truth.assertThat(testData).isEqualTo(results[0].toStringUtf8())
  }

  @Test
  fun `test writing and reading data`() = runBlocking {
    val testData = listOf("impression1", "impression2", "impression3", "impression4")
    val blobKey = "/labelled-impressions/ds/2025-02-14/event-group-id/12345/sharded-impressions"
    val testDataFlow = flow { testData.forEach { record -> emit(ByteString.copyFromUtf8(record)) } }
    shardedtorageClient.writeBlob(blobKey, testDataFlow, 2)
    val blob = shardedtorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val results = blob.read().toList()
    results.forEachIndexed { index, result ->
      Truth.assertThat(testData[index]).isEqualTo(result.toStringUtf8())
    }
  }

  @Test
  fun `test writing and reading large volume of data`() = runBlocking {
    val data =
      """{"people": [{"virtual_person_id": 1, "label": { "demo": { "gender": 1 } }}, {"virtual_person_id": 2, "label": { "demo": { "gender": 2 } }}]}"""
    val testData = List(130000) { data } // ~4MB
    val blobKey = "/labelled-impressions/ds/2025-02-14/event-group-id/12345/sharded-impressions"
    val testDataFlow = flow { testData.forEach { record -> emit(ByteString.copyFromUtf8(record)) } }
    shardedtorageClient.writeBlob(blobKey, testDataFlow, 20)
    val blob = shardedtorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val results = blob.read().toList()
    results.forEachIndexed { index, result ->
      Truth.assertThat(testData[index]).isEqualTo(result.toStringUtf8())
    }
  }
}

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

package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.securecomputation.teesdk.cloudstorage.v1alpha.RecordIoStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class RecordIoStorageClientTest {

  private lateinit var wrappedStorageClient: StorageClient
  private lateinit var recordIoStorageClient: RecordIoStorageClient

  @Before
  fun initStorageClient() {
    wrappedStorageClient = InMemoryStorageClient()
    recordIoStorageClient = RecordIoStorageClient(wrappedStorageClient)
  }

  @Test
  fun `test writing and reading single record`() = runBlocking {
    val testData = "Hello World"
    val blobKey = "test-single-record"
    recordIoStorageClient.writeBlob(
      blobKey,
      flowOf(ByteString.copyFromUtf8(testData))
    )
    val blob = recordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList()
    assertThat(1).isEqualTo(records.size)
    assertThat(testData).isEqualTo(records[0].toStringUtf8())
  }

  @Test
  fun `test writing and reading large records`() = runBlocking {
    val largeString = """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"}}}""".repeat(130000) // ~4MB
    val testData = listOf(largeString)
    val blobKey = "test-large-records"
    recordIoStorageClient.writeBlob(
      blobKey,
      testData.map { ByteString.copyFromUtf8(it) }.asFlow()
    )
    val blob = recordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList()
    assertThat(testData.size).isEqualTo(records.size)
    records.forEachIndexed { index, record ->
      assertThat(testData[index]).isEqualTo(record.toStringUtf8())
    }
  }


  @Test
  fun `test writing empty flow`() = runBlocking {
    val blobKey = "test-empty-flow"
    recordIoStorageClient.writeBlob(blobKey, emptyFlow())
    val blob = recordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList()
    assertThat(records).isEmpty()
  }

  @Test
  fun `test deleting blob`() = runBlocking {
    val blobKey = "test-delete"
    val testData = "Test Data"
    recordIoStorageClient.writeBlob(
      blobKey,
      flowOf(ByteString.copyFromUtf8(testData))
    )
    val blob = recordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    blob.delete()
    val deletedBlob = recordIoStorageClient.getBlob(blobKey)
    assertThat(deletedBlob).isNull()
  }

  @Test
  fun `test non-existent blob returns null`() = runBlocking {
    val nonExistentBlob = recordIoStorageClient.getBlob("non-existent-key")
    assertThat(nonExistentBlob).isNull()
  }
}


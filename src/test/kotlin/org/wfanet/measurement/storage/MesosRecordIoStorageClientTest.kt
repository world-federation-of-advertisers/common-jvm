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

package org.wfanet.measurement.storage

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.ComplexMessage
import org.wfanet.measurement.storage.testing.ComplexMessageKt
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.storage.testing.complexMessage

@RunWith(JUnit4::class)
class MesosRecordIoStorageClientTest {

  private lateinit var wrappedStorageClient: StorageClient
  private lateinit var mesosRecordIoStorageClient: MesosRecordIoStorageClient

  @Before
  fun initStorageClient() {
    wrappedStorageClient = InMemoryStorageClient()
    mesosRecordIoStorageClient = MesosRecordIoStorageClient(wrappedStorageClient)
  }

  @Test
  fun `test writing and reading single record`() = runBlocking {
    val testData = "Hello World"
    val blobKey = "test-single-record"
    mesosRecordIoStorageClient.writeBlob(blobKey, flowOf(ByteString.copyFromUtf8(testData)))
    val blob = mesosRecordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList()
    assertThat(1).isEqualTo(records.size)
    assertThat(testData).isEqualTo(records[0].toStringUtf8())
  }

  @Test
  fun `test writing and reading large records`() = runBlocking {
    val singleRecord =
      """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"}}}"""
    val testData = List(130000) { singleRecord } // ~4MB
    val blobKey = "test-large-records"
    val recordFlow = flow { testData.forEach { record -> emit(ByteString.copyFromUtf8(record)) } }
    mesosRecordIoStorageClient.writeBlob(blobKey, recordFlow)
    val blob = mesosRecordIoStorageClient.getBlob(blobKey)
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
    mesosRecordIoStorageClient.writeBlob(blobKey, emptyFlow())
    val blob = mesosRecordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList()
    assertThat(records).isEmpty()
  }

  @Test
  fun `test deleting blob`() = runBlocking {
    val blobKey = "test-delete"
    val testData = "Test Data"
    mesosRecordIoStorageClient.writeBlob(blobKey, flowOf(ByteString.copyFromUtf8(testData)))
    val blob = mesosRecordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    blob.delete()
    val deletedBlob = mesosRecordIoStorageClient.getBlob(blobKey)
    assertThat(deletedBlob).isNull()
  }

  @Test
  fun `test non-existent blob returns null`() = runBlocking {
    val nonExistentBlob = mesosRecordIoStorageClient.getBlob("non-existent-key")
    assertThat(nonExistentBlob).isNull()
  }

  @Test
  fun `test reading records written directly to wrapped storage client in RecordIO format`() =
    runBlocking {
      val blobKey = "test-direct-write"
      val testRecords = listOf("First record", "Second record with more content", "Third record")
      val formattedContent = buildString {
        testRecords.forEach { record ->
          val recordBytes = record.toByteArray(Charsets.UTF_8)
          append(recordBytes.size)
          append("\n")
          append(record)
        }
      }

      wrappedStorageClient.writeBlob(blobKey, flowOf(ByteString.copyFromUtf8(formattedContent)))

      val blob = mesosRecordIoStorageClient.getBlob(blobKey)
      requireNotNull(blob) { "Blob should exist" }
      val readRecords = blob.read().map { it.toStringUtf8() }.toList()

      assertThat(readRecords).hasSize(testRecords.size)
      testRecords.forEachIndexed { index, expectedRecord ->
        assertThat(readRecords[index]).isEqualTo(expectedRecord)
        val actualSize = readRecords[index].toByteArray(Charsets.UTF_8).size
        val expectedSize = expectedRecord.toByteArray(Charsets.UTF_8).size
        assertThat(actualSize).isEqualTo(expectedSize)
      }
      val rawContent =
        (wrappedStorageClient.getBlob(blobKey)!!.read().toList().first()).toStringUtf8()
      var position = 0
      testRecords.forEach { record ->
        val recordSize = record.toByteArray(Charsets.UTF_8).size
        val sizeString = recordSize.toString()
        val expectedPrefix = sizeString + "\n"

        val actualPrefix = rawContent.substring(position, position + expectedPrefix.length)
        assertThat(actualPrefix).isEqualTo(expectedPrefix)
        position += expectedPrefix.length + recordSize
      }
    }

  @Test
  fun `test reading invalid RecordIO format throws exception`() = runBlocking {
    val blobKey = "test-invalid-format"

    val invalidFormats =
      listOf(
        "aaa\nJust some content without size prefix",
        "abc\nsome content",
        "100\nshort content",
        "\n\nsome content",
        "-5\ncontent",
      )

    invalidFormats.forEachIndexed { index, invalidContent ->
      val testBlobKey = "$blobKey-$index"
      wrappedStorageClient.writeBlob(testBlobKey, flowOf(ByteString.copyFromUtf8(invalidContent)))
      val blob = mesosRecordIoStorageClient.getBlob(testBlobKey)
      requireNotNull(blob) { "Blob should exist" }

      when {
        !invalidContent.startsWith("100") -> {
          assertFailsWith<IllegalArgumentException> { blob.read().collect {} }
        }
        else -> {
          blob.read().collect {}
        }
      }
    }
  }

  @Test
  fun `test writing and reading multiple complex records`() = runBlocking {
    val testSubMessage =
      ComplexMessageKt.subMessage {
        field1 += listOf(1, 2, 3)
        field2 = ComplexMessage.Enum.STATE_2
        field3 = (1..1000).map { ('a'..'z').random() }.joinToString("")
        field4 = 100L
      }
    val testData = complexMessage {
      field1 += listOf(1, 2, 3)
      field2 += listOf(testSubMessage, testSubMessage)
      field3 = 100.0
    }
    val numRecords = 2
    val blobKey = "test-single-record"
    val data: Flow<ByteString> = flow { repeat(numRecords) { emit(testData.toByteString()) } }
    mesosRecordIoStorageClient.writeBlob(blobKey, data)
    val blob = mesosRecordIoStorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList()
    assertThat(records.size).isEqualTo(numRecords)
    for (record in records) {
      assertThat(testData).isEqualTo(ComplexMessage.parseFrom(record))
    }
  }

  @Test
  fun `test reading blob when record size and record delimiter are splitted across two chunks`() =
    runBlocking {
      val payload = "my-payload"
      val sizeStr = payload.length.toString()
      val chunk1 = ByteString.copyFromUtf8(sizeStr)
      val chunk2 = ByteString.copyFromUtf8("\n" + payload)
      val client = makeClientWithChunks(chunk1, chunk2)
      val blob = client.getBlob("fake-blob-key")!!
      val records = blob.read().toList()
      assertThat(records).hasSize(1)
      assertThat(records[0].toStringUtf8()).isEqualTo(payload)
    }

  @Test
  fun `test reading blob when record size is splitted across two chunks`() = runBlocking {
    val payload = "my-payload"
    val sizeStr = payload.length.toString()
    val chunk1 = ByteString.copyFromUtf8(sizeStr.take(2))
    val chunk2 = ByteString.copyFromUtf8(sizeStr.drop(2) + "\n" + payload)
    val client = makeClientWithChunks(chunk1, chunk2)
    val blob = client.getBlob("fake-blob-key")!!
    val records = blob.read().toList()
    assertThat(records).hasSize(1)
    assertThat(records[0].toStringUtf8()).isEqualTo(payload)
  }

  private fun makeClientWithChunks(vararg chunks: ByteString) =
    MesosRecordIoStorageClient(
      object : StorageClient {
        override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>) =
          throw UnsupportedOperationException("not used")

        override suspend fun getBlob(blobKey: String) = FakeBlob(chunks.toList())

        override suspend fun listBlobs(prefix: String?) =
          throw UnsupportedOperationException("not used")
      }
    )

  private class FakeBlob(private val chunks: List<ByteString>) : StorageClient.Blob {
    override val blobKey: String = "fake"
    override val size: Long = chunks.sumOf { it.size().toLong() }
    override val storageClient: StorageClient
      get() = throw UnsupportedOperationException("n/a")

    override fun read(): Flow<ByteString> = flow { for (c in chunks) emit(c) }

    override suspend fun delete() = Unit
  }
}

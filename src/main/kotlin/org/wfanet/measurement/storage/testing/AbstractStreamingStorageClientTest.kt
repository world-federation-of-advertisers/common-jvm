// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.storage.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import kotlin.random.Random
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.size
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

//const val TARGET_SIZE = 1024 * 1024 * 3
const val TARGET_SIZE = 228
/** Abstract base class for testing implementations of [StorageClient]. */
abstract class AbstractStreamingStorageClientTest<T : StorageClient> {
  protected open val testBlobContent: Flow<ByteString>
    get() = createTestBlobContentFlow()

  protected lateinit var storageClient: T

  suspend fun collectFlowIntoByteString(flow: Flow<ByteString>): ByteString {
    val outputStream = ByteArrayOutputStream()
    flow.collect { byteString ->
      val resultString = byteString.toString(StandardCharsets.UTF_8)
      outputStream.write(byteString.toByteArray())
    }
    return ByteString.copyFrom(outputStream.toByteArray())
  }

  @Test
  fun `Blob delete deletes blob`() = runBlocking {
    val blobKey = "blob-to-delete"
    val blob = storageClient.writeBlob(blobKey, testBlobContent)

    blob.delete()

    assertThat(storageClient.getBlob(blobKey)).isNull()
  }

  @Test
  fun `Write and read empty blob`() = runBlocking {
    val blobKey = "empty-blob"
    storageClient.writeBlob(blobKey, emptyFlow())
    val blob = assertNotNull(storageClient.getBlob(blobKey))

    assertThat(blob).contentEqualTo(ByteString.EMPTY)
  }

  companion object {
    fun createTestBlobContentFlow(): Flow<ByteString> = flow {
      val record1 = """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"},"heartbeat_interval_seconds":15.0}}"""
      val record2 = """{"type":"HEARTBEAT"}"""
      val records = listOf(record1, record2)

      var currentSize = 0

      while (currentSize < TARGET_SIZE) {
        records.forEach { record ->
          val recordBytes = record.toByteArray(Charsets.UTF_8)
          emit(ByteString.copyFrom(recordBytes)) // Emit each record as ByteString
          currentSize += recordBytes.size

          if (currentSize >= TARGET_SIZE) {
            return@flow
          }
        }
      }
    }
  }
}

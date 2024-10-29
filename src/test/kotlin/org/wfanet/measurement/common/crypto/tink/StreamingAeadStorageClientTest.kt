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
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.BlobSubject
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class StreamingAeadStorageClientTest {

  private lateinit var wrappedStorageClient: StorageClient
  private lateinit var streamingAeadStorageClient: StreamingAeadStorageClient

  @Before
  fun initStorageClient() {
    wrappedStorageClient = InMemoryStorageClient()
    streamingAeadStorageClient = StreamingAeadStorageClient(wrappedStorageClient, streamingAead)
  }

  @Test
  fun `test write and read single record`() = runBlocking {
    val blobKey = "test-key"
    val record =
      """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"}}}"""
    val inputFlow = flow { emit(ByteString.copyFromUtf8(record)) }
    val blob = streamingAeadStorageClient.writeBlob(blobKey, inputFlow)
    val readRecords = mutableListOf<String>()
    blob.read().collect { byteString -> readRecords.add(byteString.toStringUtf8()) }
    assertThat(readRecords).hasSize(1)
    assertThat(readRecords[0]).isEqualTo(record)
  }

  @Test
  fun `test write and read multiple records`() = runBlocking {
    val blobKey = "test-key"
    val records =
      listOf(
        """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"}}}""",
        """{"type":"HEARTBEAT"}""",
        """{"type":"HEARTBEAT_ACK"}""",
      )
    val combinedRecords = records.joinToString("")
    val inputFlow = flow { records.forEach { record -> emit(ByteString.copyFromUtf8(record)) } }
    val blob = streamingAeadStorageClient.writeBlob(blobKey, inputFlow)
    val combinedContent = ByteArrayOutputStream()
    blob.read().collect { byteString -> combinedContent.write(byteString.toByteArray()) }
    val readContent = combinedContent.toString(Charsets.UTF_8)
    assertThat(readContent).isEqualTo(combinedRecords)
  }

  @Test
  fun `test write and read large records`() = runBlocking {
    val blobKey = "test-key"

    val largeRecord = buildString {
      repeat(130000) { // ~ 4MB
        append("""{"type": "LARGE_RECORD", "index": $it},""")
      }
    }
    val inputFlow = flow { emit(ByteString.copyFromUtf8(largeRecord)) }
    val blob = streamingAeadStorageClient.writeBlob(blobKey, inputFlow)
    val combinedContent = ByteArrayOutputStream()
    blob.read().collect { byteString -> combinedContent.write(byteString.toByteArray()) }
    val readContent = combinedContent.toString(Charsets.UTF_8)
    assertThat(readContent).isEqualTo(largeRecord)
  }

  @Test
  fun `wrapped blob is encrypted`() = runBlocking {
    val blobKey = "test-blob"
    val testContent = """{"type": "TEST_RECORD", "data": "test content"}"""
    val inputFlow = flow { emit(ByteString.copyFromUtf8(testContent)) }
    streamingAeadStorageClient.writeBlob(blobKey, inputFlow)
    val encryptedBlob = wrappedStorageClient.getBlob(blobKey)
    val decryptedContent = ByteArrayOutputStream()
    val decryptingChannel =
      streamingAead.newDecryptingChannel(
        Channels.newChannel(ByteArrayInputStream(encryptedBlob?.read()?.first()?.toByteArray())),
        blobKey.encodeToByteArray(),
      )
    val buffer = ByteBuffer.allocate(8192)
    while (decryptingChannel.read(buffer) != -1) {
      buffer.flip()
      decryptedContent.write(buffer.array(), 0, buffer.limit())
      buffer.clear()
    }
    assertThat(String(decryptedContent.toByteArray())).isEqualTo(testContent)
  }

  @Test
  fun `Blob delete deletes blob`() = runBlocking {
    val blobKey = "blob-to-delete"
    val record =
      """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"}}}"""
    val inputFlow = flow { emit(ByteString.copyFromUtf8(record)) }
    val blob = streamingAeadStorageClient.writeBlob(blobKey, inputFlow)
    blob.delete()
    assertThat(streamingAeadStorageClient.getBlob(blobKey)).isNull()
  }

  @Test
  fun `Write and read empty blob`() = runBlocking {
    val blobKey = "empty-blob"
    streamingAeadStorageClient.writeBlob(blobKey, emptyFlow())
    val blob = assertNotNull(streamingAeadStorageClient.getBlob(blobKey))
    BlobSubject.assertThat(blob).contentEqualTo(ByteString.EMPTY)
  }

  companion object {

    init {
      StreamingAeadConfig.register()
    }

    private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM_HKDF_1MB")
    private val KEY_ENCRYPTION_KEY = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
    private val streamingAead = KEY_ENCRYPTION_KEY.getPrimitive(StreamingAead::class.java)
  }
}

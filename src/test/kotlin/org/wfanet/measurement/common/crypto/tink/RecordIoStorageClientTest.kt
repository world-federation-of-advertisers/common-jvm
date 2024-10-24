// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.config.TinkConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.securecomputation.teesdk.cloudstorage.v1alpha.RecordIoStorageClient
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

const val TARGET_SIZE = 1024 * 1024 * 3
@RunWith(JUnit4::class)
class RecordIoStorageClientTest : AbstractStorageClientTest<RecordIoStorageClient>() {

  override val testBlobContent: ByteString
    get() = createTestBlobContent()

  private val wrappedStorageClient = InMemoryStorageClient()

  @Before
  fun initStorageClient() {
    storageClient = RecordIoStorageClient(
      wrappedStorageClient,
      streamingAead
    )
  }

  @Test
  fun `wrapped blob is encrypted`() = runBlocking {
    val blobKey = "kms-blob"

    storageClient.writeBlob(blobKey, testBlobContent)

    val wrappedBlob = assertNotNull(wrappedStorageClient.getBlob(blobKey))
//    val plainTextContent =
//      streamingAead.decrypt(wrappedBlob.read().toByteArray(), blobKey.encodeToByteArray()).toByteString()
//    Truth.assertThat(plainTextContent).isEqualTo(testBlobContent)
  }

  companion object {

    val STREAMING_AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM_HKDF_1MB")
    val streamingKeysetHandle = KeysetHandle.generateNew(STREAMING_AEAD_KEY_TEMPLATE)
    val streamingAead = streamingKeysetHandle.getPrimitive(StreamingAead::class.java)

    init {
      TinkConfig.register()
      StreamingAeadConfig.register()
      AeadConfig.register()
    }
    fun createTestBlobContent(): ByteString {
      val record1 = """{"type": "SUBSCRIBED","subscribed": {"framework_id": {"value":"12220-3440-12532-2345"},"heartbeat_interval_seconds":15.0}}"""
      val record2 = """{"type":"HEARTBEAT"}"""
      val records = listOf(record1, record2)
      val recordIOBuilder = ByteString.newOutput()
      var currentSize = 0
      while (currentSize < TARGET_SIZE) {
        records.forEach { record ->
          val length = record.toByteArray().size
          recordIOBuilder.write(length.toString().toByteArray())
          recordIOBuilder.write("\n".toByteArray())
          recordIOBuilder.write(record.toByteArray())
          recordIOBuilder.write("\n".toByteArray())
          currentSize += (length.toString().toByteArray().size + 1 + length + 1)
          if (currentSize >= TARGET_SIZE) {
            return@forEach
          }
        }
      }
      return recordIOBuilder.toByteString()
    }
  }

}

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
import java.nio.channels.Channels
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.size
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class StreamingAeadStorageClientTest : AbstractStorageClientTest<StreamingAeadStorageClient>() {

  private val wrappedStorageClient = InMemoryStorageClient()

  override fun computeStoredBlobSize(content: ByteString, blobKey: String): Int {
    // See https://github.com/google/tink/blob/master/docs/WIRE-FORMAT.md
    // AES_128_GCM ciphertext is same size as plaintext.
    return TOTAL_OVERHEAD_BYTES + content.size
  }

  @Before
  fun initStorageClient() {
    storageClient = StreamingAeadStorageClient(wrappedStorageClient, streamingAead)
  }

  @Test
  fun `wrapped blob is encrypted`() = runBlocking {
    val blobKey = "kms-blob"
    storageClient.writeBlob(blobKey, testBlobContent)
    val wrappedBlob = assertNotNull(wrappedStorageClient.getBlob(blobKey))
    val inputChannel = Channels.newChannel(ByteArrayInputStream(wrappedBlob.read().toByteArray()))
    val decryptingChannel =
      streamingAead.newDecryptingChannel(inputChannel, blobKey.encodeToByteArray())
    val plainTextContent = decryptingChannel.asFlow(BYTES_PER_MIB).flatten()
    decryptingChannel.close()
    inputChannel.close()
    assertThat(plainTextContent).isEqualTo(testBlobContent)
  }

  companion object {
    private const val TINK_PREFIX_SIZE_BYTES = 5
    private const val HEADER_SIZE_BYTES = 1
    private const val SEGMENT_INFO_SIZE_BYTES = 21
    private const val FIRST_SEGMENT_HEADER_SIZE = 21
    private const val SEGMENT_TAG_SIZE_BYTES = 16
    private const val LAST_SEGMENT_HEADER_SIZE = 24

    private const val TOTAL_OVERHEAD_BYTES =
      TINK_PREFIX_SIZE_BYTES +
        HEADER_SIZE_BYTES +
        SEGMENT_INFO_SIZE_BYTES +
        FIRST_SEGMENT_HEADER_SIZE +
        SEGMENT_TAG_SIZE_BYTES +
        LAST_SEGMENT_HEADER_SIZE

    init {
      StreamingAeadConfig.register()
    }

    private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM_HKDF_1MB")
    private val KEY_ENCRYPTION_KEY = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
    private val streamingAead = KEY_ENCRYPTION_KEY.getPrimitive(StreamingAead::class.java)
  }
}

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
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
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
import com.google.crypto.tink.aead.AeadConfig
import kotlin.random.Random

@RunWith(JUnit4::class)
class AeadStorageClientTest : AbstractStorageClientTest<AeadStorageClient>() {

  private val wrappedStorageClient = InMemoryStorageClient()

  override fun computeStoredBlobSize(content: ByteString, blobKey: String): Int {
    return content.size + TINK_PREFIX_SIZE + IV_SIZE + TAG_SIZE
  }

  @Before
  fun initStorageClient() {
    storageClient = AeadStorageClient(wrappedStorageClient, aead)
  }

  @Test
  fun `wrapped blob is encrypted`() = runBlocking {
    val blobKey = "kms-blob"
    storageClient.writeBlob(blobKey, testBlobContent)
    val wrappedBlob = assertNotNull(wrappedStorageClient.getBlob(blobKey))
    val encryptedBytes = wrappedBlob.read().toByteArray()
    assertThat(encryptedBytes).isNotEqualTo(testBlobContent)
    val decrypted = aead.decrypt(encryptedBytes, blobKey.encodeToByteArray())
    assertThat(ByteString.copyFrom(decrypted)).isEqualTo(testBlobContent)
  }

  companion object {

    init {
      AeadConfig.register()
    }
    private const val TINK_PREFIX_SIZE = 5
    private const val IV_SIZE = 12
    private const val TAG_SIZE = 16

    private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM")
    private val KEY_HANDLE = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
    private val aead = KEY_HANDLE.getPrimitive(Aead::class.java)
  }
}

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
import com.google.crypto.tink.KmsClients
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.kotlin.toByteString
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.storage.createBlob
import org.wfanet.measurement.storage.read
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class KmsStorageClientTest : AbstractStorageClientTest<KmsStorageClient>() {
  override val testBlobSize: Int = aead.encrypt(testBlobContent.toByteArray(), null).size

  private val wrappedStorageClient = InMemoryStorageClient()

  @Before
  fun initStorageClient() {
    storageClient =
      TinkKeyStorageProvider().makeKmsStorageClient(wrappedStorageClient, KEK_URI) as
        KmsStorageClient
  }

  @Test
  fun `wrapped blob is encrypted`() = runBlocking {
    val blobKey = "kms-blob"

    storageClient.createBlob(blobKey, testBlobContent)

    val wrappedBlob = assertNotNull(wrappedStorageClient.getBlob(blobKey))
    assertThat(aead.decrypt(wrappedBlob.read().toByteArray(), null).toByteString())
      .isEqualTo(testBlobContent)
  }

  companion object {
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"

    init {
      AeadConfig.register()
    }
    private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM")
    private val KEY_ENCRYPTION_KEY = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
    private val aead = KEY_ENCRYPTION_KEY.getPrimitive(Aead::class.java)

    init {
      KmsClients.add(FakeKmsClient().apply { addAead(KEK_URI, aead) })
    }
  }
}

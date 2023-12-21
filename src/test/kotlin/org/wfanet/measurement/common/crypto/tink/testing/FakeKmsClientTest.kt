/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto.tink.testing

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import java.security.GeneralSecurityException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Tests for [FakeKmsClient]. */
@RunWith(JUnit4::class)
class FakeKmsClientTest {
  @Test
  fun `getAead returns Aead for supported key URI`() {
    val keyUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val keyHandle = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
    val aead = keyHandle.getPrimitive(Aead::class.java)
    val kmsClient = FakeKmsClient()
    kmsClient.setAead(keyUri, aead)
    val plainText = "lorem ipsum".toByteArray(Charsets.UTF_8)

    val registeredAead = kmsClient.getAead(keyUri)
    val cipherText: ByteArray = aead.encrypt(plainText, null)
    assertThat(registeredAead.decrypt(cipherText, null)).isEqualTo(plainText)
  }

  @Test
  fun `getAead throws GeneralSecurityException for unsupported key URI`() {
    val keyUri = FakeKmsClient.KEY_URI_PREFIX + "key"
    val kmsClient = FakeKmsClient()

    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(keyUri) }
  }

  companion object {
    init {
      AeadConfig.register()
    }

    private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM")
  }
}

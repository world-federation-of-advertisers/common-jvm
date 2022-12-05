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
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.testing.TestData

@RunWith(JUnit4::class)
class KeyHandleTest {
  @Test
  fun `generated private key can decrypt value`() {
    val privateKey = TinkPrivateKeyHandle.generateEcies()

    val cipherText = privateKey.publicKey.hybridEncrypt(PLAIN_TEXT_MESSAGE_BINARY)
    assertThat(privateKey.hybridDecrypt(cipherText)).isEqualTo(PLAIN_TEXT_MESSAGE_BINARY)
  }

  @Test
  fun `loaded private key can decrypt value`() {
    val privateKey = loadPrivateKey(TestData.FIXED_ENCRYPTION_PRIVATE_KEYSET)

    val cipherText = privateKey.publicKey.hybridEncrypt(PLAIN_TEXT_MESSAGE_BINARY)
    assertThat(privateKey.hybridDecrypt(cipherText)).isEqualTo(PLAIN_TEXT_MESSAGE_BINARY)
  }

  @Test
  fun `loaded private key can decrypt value encrypted by loaded public key`() {
    val privateKey = loadPrivateKey(TestData.FIXED_ENCRYPTION_PRIVATE_KEYSET)
    val publicKey = loadPublicKey(TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET)

    val cipherText = publicKey.hybridEncrypt(PLAIN_TEXT_MESSAGE_BINARY)
    assertThat(privateKey.hybridDecrypt(cipherText)).isEqualTo(PLAIN_TEXT_MESSAGE_BINARY)
  }

  companion object {
    private const val PLAIN_TEXT_MESSAGE = "Lorem ipsum dolor sit amet"
    private val PLAIN_TEXT_MESSAGE_BINARY = PLAIN_TEXT_MESSAGE.toByteStringUtf8()
  }
}

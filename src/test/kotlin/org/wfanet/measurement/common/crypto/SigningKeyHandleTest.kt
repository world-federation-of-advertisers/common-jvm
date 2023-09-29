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

package org.wfanet.measurement.common.crypto

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.testing.loadSigningKey

@RunWith(JUnit4::class)
class SigningKeyHandleTest {
  @Test
  fun `sign generates valid signature`() {
    val signingKey =
      loadSigningKey(TestData.FIXED_SERVER_CERT_DER_FILE, TestData.FIXED_SERVER_KEY_DER_FILE)

    val signature = signingKey.sign(SignatureAlgorithm.ECDSA_WITH_SHA256, MESSAGE_BINARY)

    assertThat(
        signingKey.certificate.verifySignature(
          SignatureAlgorithm.ECDSA_WITH_SHA256,
          MESSAGE_BINARY,
          signature
        )
      )
      .isTrue()
  }

  companion object {
    private const val MESSAGE = "A message to sign"
    private val MESSAGE_BINARY = MESSAGE.toByteStringUtf8()
  }
}

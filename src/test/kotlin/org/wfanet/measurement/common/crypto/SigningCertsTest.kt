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

package org.wfanet.measurement.common.crypto

import com.google.common.truth.Truth.assertThat
import java.security.cert.X509Certificate
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.testing.TestData

@RunWith(JUnit4::class)
class SigningCertsTest {
  @Test
  fun `constructor throws if trusted certificate AKID exists and SKID do not match`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        SigningCerts(SERVER_SIGNING_KEY, mapOf(SERVER_CERT.subjectKeyIdentifier!! to SERVER_CERT))
      }
    assertThat(exception).hasMessageThat().ignoringCase().contains("root")
  }

  @Test
  fun `constructor accepts SKID as AKID if AKID does not exist`() {
    SigningCerts(SERVER_SIGNING_KEY, mapOf(NO_AKID_CERT.subjectKeyIdentifier!! to NO_AKID_CERT))
  }

  companion object {
    private val SERVER_CERT: X509Certificate by lazy {
      readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)
    }
    private val NO_AKID_CERT: X509Certificate by lazy {
      readCertificate(TestData.FIXED_NO_AKID_CERT_PEM_FILE)
    }
    private val SERVER_SIGNING_KEY: SigningKeyHandle by lazy {
      val privateKey =
        readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, SERVER_CERT.publicKey.algorithm)
      SigningKeyHandle(SERVER_CERT, privateKey)
    }
  }
}

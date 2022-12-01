// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.testing.SignatureSubject.Companion.assertThat
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.flatten

private val DATA =
  ByteString.copyFromUtf8("I am some data to sign. I am some data to sign. I am some data to sign.")
private val SIGNATURE =
  HexString(
    "3046022100F7F72BFFD598D417C4E1F5265F6ECA617D1D5533FBC6C8B9662F1C08A9AD8E3A022100B93E9566DAC6" +
      "F9706C8B7A0B6A003C1B427E8757EF95F84E7AECE23BE9A91453"
  )
private val ALT_DATA = ByteString.copyFromUtf8("I am some alternative data")
private val BOGUS_SIGNATURE =
  HexString(
    "304402200D3ACC867DA66D34586E9A7B3E73B319E35F169D13EC912761A2AC287EC46C1B02201DE3FD224D6D1DE0" +
      "4FD7C5436DF41545D85ACE41B2AA5B815B4BDBB89CB33166"
  )

@RunWith(JUnit4::class)
class SignaturesTest {
  @Test
  fun `verifySignature returns true for valid signature`() {
    assertTrue(certificate.verifySignature(DATA, SIGNATURE.bytes))
  }

  @Test
  fun `verifySignature returns false when signature does not match data`() {
    assertFalse(certificate.verifySignature(ALT_DATA, SIGNATURE.bytes))
  }

  @Test
  fun `verifySignature returns false when signature does not match certificate`() {
    assertFalse(altCertificate.verifySignature(DATA, SIGNATURE.bytes))
  }

  @Test
  fun `verifyAndCollect returns true for valid signature`() {
    val dataFlow = DATA.asBufferedFlow(24)

    var collected = ByteString.EMPTY
    val verified = runBlocking {
      dataFlow.collectAndVerify(certificate, SIGNATURE.bytes) { bytes ->
        collected = collected.concat(bytes)
      }
    }

    assertThat(collected).isEqualTo(DATA)
    assertTrue(verified)
  }

  @Test
  fun `sign returns valid signature`() {
    val signature = privateKey.sign(certificate, DATA)

    assertThat(signature).isValidFor(certificate, DATA)
  }

  @Test
  fun `collectAndSign returns valid signature`() {
    val dataFlow = DATA.asBufferedFlow(24)

    var collected = ByteString.EMPTY
    val signature = runBlocking {
      dataFlow.collectAndSign({ privateKey.newSigner(certificate) }) { bytes ->
        collected = collected.concat(bytes)
      }
    }

    assertThat(collected).isEqualTo(DATA)
    assertThat(signature).isValidFor(certificate, DATA)
  }

  @Test
  fun `verifying returns flow with input data when signature is valid`() {
    val dataFlow = DATA.asBufferedFlow(24)

    val verifyingFlow = dataFlow.verifying(certificate, SIGNATURE.bytes)

    assertThat(runBlocking { verifyingFlow.flatten() }).isEqualTo(DATA)
  }

  @Test
  fun `verifying throws for invalid flow signature`() {
    val dataFlow = DATA.asBufferedFlow(24)

    val verifyingFlow = dataFlow.verifying(certificate, BOGUS_SIGNATURE.bytes)

    assertFailsWith(InvalidSignatureException::class) { runBlocking { verifyingFlow.flatten() } }
  }

  @Test
  fun `validate does not throw exception for valid certificate`() {
    certificate.validate(issuerCertificate)
  }

  @Test
  fun `validate throws exception for incorrect issuer`() {
    assertFailsWith<CertPathValidatorException> { certificate.validate(altCertificate) }
  }

  @Test
  fun `validate throws exception for expired certificate`() {
    val exception =
      assertFailsWith<CertPathValidatorException> { expiredCertificate.validate(issuerCertificate) }
    assertThat(exception.reason).isEqualTo(CertPathValidatorException.BasicReason.EXPIRED)
  }

  companion object {
    val certificate: X509Certificate = readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)
    val altCertificate: X509Certificate = readCertificate(TestData.FIXED_CLIENT_CERT_PEM_FILE)
    val expiredCertificate: X509Certificate = readCertificate(TestData.FIXED_EXPIRED_CERT_PEM_FILE)
    val issuerCertificate: X509Certificate = readCertificate(TestData.FIXED_CA_CERT_PEM_FILE)
    val privateKey: PrivateKey =
      readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, certificate.publicKey.algorithm)
  }
}

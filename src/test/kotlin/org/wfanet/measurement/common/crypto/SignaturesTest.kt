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

import com.google.common.collect.Range
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.exception.InvalidSignatureException
import org.wfanet.measurement.common.crypto.testing.*
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE as SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE as SERVER_KEY_FILE
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toByteString

private val DATA = ByteString.copyFromUtf8("I am some data to sign")
private val LONG_DATA =
  ByteString.copyFromUtf8("I am some data to sign. I am some data to sign. I am some data to sign.")
private val ALT_DATA = ByteString.copyFromUtf8("I am some alternative data")

// TODO: Consider migrating this a separate file if needed elsewhere in the repo.
// kotlinx.coroutines.test.runBlockingTest complains about
// "java.lang.IllegalStateException: This job has not completed yet".
// This is a common issue: https://github.com/Kotlin/kotlinx.coroutines/issues/1204.
private fun runBlockingTest(block: suspend CoroutineScope.() -> Unit) {
  runBlocking { block() }
}

@RunWith(JUnit4::class)
class SignaturesTest {
  @Test
  fun `sign returns signature of correct size`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)

    val signature = privateKey.sign(certificate, DATA)

    // DER-encoded ECDSA signature using 256-bit key can be 70, 71, or 72 bytes.
    assertThat(signature.size()).isIn(Range.closed(70, 72))
  }

  @Test
  fun `signFlow returns signature of correct size`() = runBlockingTest {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)

    val (outFlow, signature) = privateKey.signFlow(certificate, LONG_DATA.asBufferedFlow(24))

    assertThat(outFlow.flatten()).isEqualTo(LONG_DATA)

    // DER-encoded ECDSA signature using 256-bit key can be 70, 71, or 72 bytes.
    assertThat(signature.await().size()).isIn(Range.closed(70, 72))
  }

  @Test
  fun `verifySignature returns true for valid signature`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)
    val signature = privateKey.sign(certificate, DATA)

    assertTrue(certificate.verifySignature(DATA, signature))
  }

  @Test
  fun `verifySignedFlow returns true for valid signatures`() = runBlockingTest {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)
    val regularSig = privateKey.sign(certificate, LONG_DATA)

    val (signFlow, deferredSig) = privateKey.signFlow(certificate, LONG_DATA.asBufferedFlow(24))
    assertThat(signFlow.flatten()).isEqualTo(LONG_DATA)

    val outFlow = certificate.verifySignedFlow(LONG_DATA.asBufferedFlow(24), regularSig)
    val outFlow2 = certificate.verifySignedFlow(LONG_DATA.asBufferedFlow(24), deferredSig.await())
    assertThat(outFlow.flatten()).isEqualTo(LONG_DATA)
    assertThat(outFlow2.flatten()).isEqualTo(LONG_DATA)
  }

  @Test
  fun `verifySignature returns false for signature from different data`() {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)
    val signature = privateKey.sign(certificate, DATA)

    assertFalse(certificate.verifySignature(ALT_DATA, signature))
  }

  @Test
  fun `verifySignedFlow throws for invalid signed Flow`() = runBlockingTest {
    val privateKey = readPrivateKey(SERVER_KEY_FILE, KEY_ALGORITHM)
    val certificate: X509Certificate = readCertificate(SERVER_CERT_PEM_FILE)
    val signature = privateKey.sign(certificate, DATA)

    val outFlow = certificate.verifySignedFlow(LONG_DATA.asBufferedFlow(24), signature)
    assertFailsWith(InvalidSignatureException::class) {
      assertThat(outFlow.flatten()).isEqualTo(LONG_DATA)
    }

    val (signFlow, deferredSig) = privateKey.signFlow(certificate, DATA.asBufferedFlow(24))
    assertThat(signFlow.flatten()).isEqualTo(DATA)

    val outFlow2 = certificate.verifySignedFlow(LONG_DATA.asBufferedFlow(24), deferredSig.await())
    assertFailsWith(InvalidSignatureException::class) { outFlow2.collect() }
  }

  @Test
  fun `verifyDoubleSignature returns false when only first signature is valid`() {
    val certificate1: X509Certificate = readCertificate(ORG1_SERVER_CERT_PEM_FILE)
    val certificate2: X509Certificate = readCertificate(ORG2_SERVER_CERT_PEM_FILE)
    val privateKey1 = readPrivateKey(ORG1_SERVER_CERT_KEY_FILE, KEY_ALGORITHM)
    val signature1 = privateKey1.sign(certificate1, DATA)

    assertFalse(
      verifyDoubleSignature(DATA, signature1, randomSignature, certificate1, certificate2)
    )
  }

  @Test
  fun `verifyDoubleSignature returns false when only second signature is valid`() {
    val certificate1: X509Certificate = readCertificate(ORG1_SERVER_CERT_PEM_FILE)
    val certificate2: X509Certificate = readCertificate(ORG2_SERVER_CERT_PEM_FILE)
    val privateKey2 = readPrivateKey(ORG2_SERVER_CERT_KEY_FILE, KEY_ALGORITHM)
    val signature2 = privateKey2.sign(certificate2, DATA)

    assertFalse(
      verifyDoubleSignature(DATA, randomSignature, signature2, certificate1, certificate2)
    )
  }

  @Test
  fun `verifyDoubleSignature returns true when both signatures are valid`() {
    val certificate1: X509Certificate = readCertificate(ORG1_SERVER_CERT_PEM_FILE)
    val certificate2: X509Certificate = readCertificate(ORG2_SERVER_CERT_PEM_FILE)
    val privateKey1 = readPrivateKey(ORG1_SERVER_CERT_KEY_FILE, KEY_ALGORITHM)
    val privateKey2 = readPrivateKey(ORG2_SERVER_CERT_KEY_FILE, KEY_ALGORITHM)
    val signature1 = privateKey1.sign(certificate1, DATA)
    val signature2 = privateKey2.sign(certificate2, DATA)

    assertTrue(
      verifyDoubleSignature(DATA, signature1, signature2, certificate1, certificate2)
    )
  }

  companion object {
    private val random = Random.Default
    private val randomSignature: ByteString = random.nextBytes(70).toByteString()
  }
}

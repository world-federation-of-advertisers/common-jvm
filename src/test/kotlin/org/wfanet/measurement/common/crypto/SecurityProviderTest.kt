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
import java.security.cert.X509Certificate
import java.security.spec.InvalidKeySpecException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.common.crypto.testing.FIXED_CA_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_CLIENT_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE

private const val KEY_ALGORITHM = "EC"
private val SERVER_SKID =
  byteStringOf(
    0xE7,
    0xB3,
    0xB5,
    0x45,
    0x77,
    0x1B,
    0xC2,
    0xB9,
    0xA1,
    0x88,
    0x02,
    0x07,
    0x90,
    0x3F,
    0x87,
    0xA5,
    0xC4,
    0x2C,
    0x63,
    0xA8
  )
private val CLIENT_SKID =
  byteStringOf(
    0x48,
    0x32,
    0x98,
    0xE2,
    0x03,
    0xFE,
    0xA1,
    0xAF,
    0xA0,
    0x8D,
    0x10,
    0x7C,
    0x92,
    0x37,
    0xCE,
    0x19,
    0x11,
    0x6A,
    0xA7,
    0x8F,
  )
private val CLIENT_AKID =
  byteStringOf(
    0x57,
    0xE8,
    0x9A,
    0x06,
    0x76,
    0xBE,
    0xBA,
    0x1E,
    0xA0,
    0x71,
    0x50,
    0x5C,
    0x40,
    0x87,
    0x9B,
    0x98,
    0xF1,
    0xF5,
    0x0C,
    0x9E,
  )

@RunWith(JUnit4::class)
class SecurityProviderTest {
  @Test
  fun `readCertificate reads fixed cert from PEM file`() {
    val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)

    assertThat(certificate.subjectDN.name).isEqualTo("CN=server.example.com,O=Server")
  }

  @Test
  fun `readPrivateKey reads key from PKCS#8 PEM file`() {
    val privateKey = readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)

    assertThat(privateKey.format).isEqualTo("PKCS#8")
  }

  @Test
  fun `readPrivateKey reads key from PKCS#8 PEM ByteString`() {
    val privateKey = readPrivateKey(FIXED_SERVER_KEY_FILE, KEY_ALGORITHM)
    val data = ByteString.copyFrom(privateKey.getEncoded())
    val privateKeyCopy = readPrivateKey(data, KEY_ALGORITHM)
    assertThat(privateKeyCopy.format).isEqualTo("PKCS#8")
    assertThat(privateKey).isEqualTo(privateKeyCopy)
  }

  @Test
  fun `readPrivateKey reads key from invalid encoded ByteString`() {
    val data = ByteString.copyFromUtf8("some-invalid-encoded-key")
    assertFailsWith(InvalidKeySpecException::class) { readPrivateKey(data, KEY_ALGORITHM) }
  }

  @Test
  fun `subjectKeyIdentifier returns SKID`() {
    val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)

    assertThat(certificate.subjectKeyIdentifier).isEqualTo(SERVER_SKID)
  }

  @Test
  fun `authorityKeyIdentifier returns SKID of issuer`() {
    val issuerCertificate = readCertificate(FIXED_CA_CERT_PEM_FILE)
    val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)

    assertThat(certificate.authorityKeyIdentifier).isEqualTo(issuerCertificate.subjectKeyIdentifier)
  }

  @Test
  fun `subjectKeyIdentifier returns SKID of certificate with another format`() {
    val certificate: X509Certificate = readCertificate(FIXED_CLIENT_CERT_PEM_FILE)

    assertThat(certificate.subjectKeyIdentifier).isEqualTo(CLIENT_SKID)
  }

  @Test
  fun `authorityKeyIdentifier returns AKID of certificate with another format`() {
    val certificate: X509Certificate = readCertificate(FIXED_CLIENT_CERT_PEM_FILE)

    assertThat(certificate.authorityKeyIdentifier).isEqualTo(CLIENT_AKID)
  }
}

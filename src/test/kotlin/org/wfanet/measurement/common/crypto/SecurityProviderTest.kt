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
import org.wfanet.measurement.common.crypto.testing.DYNAMIC_SERVER_1_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_CA_CERT_PEM_FILE
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

@RunWith(JUnit4::class)
class SecurityProviderTest {
  @Test
  fun `readCertificate reads fixed cert from PEM file`() {
    val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)

    assertThat(certificate.subjectDN.name).isEqualTo("CN=server.example.com,O=Server")
  }

  @Test
  fun `readCertificate reads dynamically created cert from PEM file`() {
    val certificate: X509Certificate = readCertificate(DYNAMIC_SERVER_1_CERT_PEM_FILE)

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
}

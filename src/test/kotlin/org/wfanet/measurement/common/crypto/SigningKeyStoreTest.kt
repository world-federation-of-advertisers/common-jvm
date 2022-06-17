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
import java.security.cert.X509Certificate
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class SigningKeyStoreTest {
  private val storageClient = InMemoryStorageClient()
  private val signingKeyStore = SigningKeyStore(storageClient)

  @Test
  fun `write writes PEM file to storage`() = runBlocking {
    SigningKeyHandle(certificate, privateKey).write(signingKeyStore)

    val blob = storageClient.contents.values.single()
    assertThat(blob.read().flatten().toStringUtf8()).isEqualTo(CONCATENATED_PEM)
  }

  @Test
  fun `read by blob context reads signing key from store`() = runBlocking {
    val privateKey = SigningKeyHandle(certificate, privateKey)
    privateKey.write(signingKeyStore)

    val readKey: SigningKeyHandle =
      assertNotNull(signingKeyStore.read(SigningKeyStore.Context.fromCertificate(certificate)))

    assertThat(readKey).isEqualTo(privateKey)
  }

  @Test
  fun `read by blob key reads signing key from store`() = runBlocking {
    val privateKey = SigningKeyHandle(certificate, privateKey)
    val blobKey: String = privateKey.write(signingKeyStore)

    val readKey: SigningKeyHandle = assertNotNull(signingKeyStore.read(blobKey))

    assertThat(readKey).isEqualTo(privateKey)
  }

  companion object {
    private val CONCATENATED_PEM: String =
      FIXED_SERVER_CERT_PEM_FILE.readText() + FIXED_SERVER_KEY_FILE.readText()

    private val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)
    private val privateKey = readPrivateKey(FIXED_SERVER_KEY_FILE, certificate.publicKey.algorithm)
  }
}

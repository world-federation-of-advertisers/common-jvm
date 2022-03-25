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
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_PEM_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_FILE
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.Store
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class SigningKeyStoreTest {
  @get:Rule val tempDir = TemporaryFolder()

  private lateinit var store: Store<SigningKeyStore.Context>
  private lateinit var signingKeyStore: SigningKeyStore

  @Before
  fun initSigningKeyStore() {
    val storageClient = FileSystemStorageClient(tempDir.root)
    store =
      object : Store<SigningKeyStore.Context>(storageClient) {
        override val blobKeyPrefix: String = "sigKeys"

        override fun deriveBlobKey(context: SigningKeyStore.Context): String = context.blobKey
      }
    signingKeyStore = SigningKeyStore(store)
  }

  @Test
  fun `write writes PEM file to store`() = runBlocking {
    val keyId: String = SigningKeyHandle(certificate, privateKey).write(signingKeyStore)

    val blob = assertNotNull(store.get(keyId))
    assertThat(blob.read().flatten().toStringUtf8()).isEqualTo(CONCATENATED_PEM)
  }

  @Test
  fun `read reads signing key from store`() = runBlocking {
    val privateKey = SigningKeyHandle(certificate, privateKey)
    val keyId: String = privateKey.write(signingKeyStore)

    val readKey: SigningKeyHandle = assertNotNull(signingKeyStore.read(keyId))

    assertThat(readKey).isEqualTo(privateKey)
  }

  companion object {
    private val CONCATENATED_PEM: String =
      FIXED_SERVER_CERT_PEM_FILE.readText() + FIXED_SERVER_KEY_FILE.readText()

    private val certificate: X509Certificate = readCertificate(FIXED_SERVER_CERT_PEM_FILE)
    private val privateKey = readPrivateKey(FIXED_SERVER_KEY_FILE, certificate.publicKey.algorithm)
  }
}

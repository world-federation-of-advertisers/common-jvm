// Copyright 2023 The Cross-Media Measurement Authors
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
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.SignedStore.BlobNotFoundException
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import src.main.kotlin.org.wfanet.measurement.common.crypto.EncryptedSignedStore

@RunWith(JUnit4::class)
class EncryptedSignedStoreTest {
  @Test
  fun `write and read model blob to storage`() = runBlocking {
    println("joji's world")
    val privateKey = TinkPrivateKeyHandle.generateEcies(true)
    val publicKey = privateKey.publicKey
    // write blob
    encryptedSignedStore.write(
      blobKey,
      signingX509,
      signingPrivateKey,
      publicKey,
      DATA.asBufferedFlow(24)
    )

    // read blob
    val readData = encryptedSignedStore.read(blobKey, signingX509, privateKey)

    assertThat(readData?.flatten()?.toStringUtf8()).isEqualTo(DATA.toStringUtf8())
  }

//  @Test
//  fun `write and read model blob to storage throws BlobNotFoundException if blobKey is not valid`() =
//    runBlocking {
//      // write blob
//      encryptedSignedStore.write(
//        blobKey,
//        signingX509,
//        signingPrivateKey,
//        encryptingPublicKeyHandle,
//        DATA.asBufferedFlow(24)
//      )
//
//      val invalidBlobKey = "invalid-blob-key"
//
//      // read blob
//      val exception =
//        assertFailsWith<BlobNotFoundException> {
//          encryptedSignedStore.read(invalidBlobKey, signingX509, decryptingPrivateKeyHandle)
//        }
//
//      assertThat(exception).hasMessageThat().ignoringCase().contains("$invalidBlobKey not found")
//    }

  companion object {
    private val storageClient = InMemoryStorageClient()

    private val encryptedSignedStore = EncryptedSignedStore(storageClient)

    private val DATA =
      ByteString.copyFromUtf8("I am a model blob. I am a model blob. I am a model blob.")

    private const val blobKey = "blob-key"

    private val signingX509: X509Certificate = readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)

    private val signingPrivateKey =
      readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, signingX509.publicKey.algorithm)

    private val encryptingPublicKeyHandle = loadPublicKey(TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET)

    private val decryptingPrivateKeyHandle =
      loadPrivateKey(TestData.FIXED_ENCRYPTION_PRIVATE_KEYSET)
  }
}

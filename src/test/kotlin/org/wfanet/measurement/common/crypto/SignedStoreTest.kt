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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.SignedStore.BlobNotFoundException
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class SignedStoreTest {
  @Test
  fun `write and read model blob to storage`() = runBlocking {
    // write blob
    signedStore.write(blobKey, certificate, privateKey, DATA.asBufferedFlow(24))

    // read blob
    val readData = signedStore.read(blobKey, certificate)
    assertThat(readData.flatten().toStringUtf8())
      .isEqualTo(DATA.asBufferedFlow(24).flatten().toStringUtf8())
  }

  @Test
  fun `write and read model blob to storage throws BlobNotFoundException if blobKey is not valid`() =
    runBlocking {
      // write blob
      val blobKey = "blob-key"

      signedStore.write(blobKey, certificate, privateKey, DATA.asBufferedFlow(24))

      val invalidBlobKey = "invalid-blob-key"
      // read blob
      val exception =
        assertFailsWith<BlobNotFoundException> { signedStore.read(invalidBlobKey, certificate) }

      assertThat(exception).hasMessageThat().ignoringCase().contains("$invalidBlobKey not found")
    }

  companion object {
    private val storageClient = InMemoryStorageClient()

    private val signedStore = SignedStore(storageClient)

    private val DATA =
      ByteString.copyFromUtf8("I am a model blob. I am a model blob. I am a model blob.")

    private const val blobKey = "blob-key"

    private val certificate: X509Certificate = readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)

    private val privateKey =
      readPrivateKey(TestData.FIXED_SERVER_KEY_FILE, certificate.publicKey.algorithm)
  }
}

// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.aws.kms

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import java.security.GeneralSecurityException
import java.util.concurrent.CompletionException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock

private const val KEY_URI = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key-id"

@RunWith(JUnit4::class)
class CompletionExceptionTranslatingKmsClientTest {

  @Test
  fun `encrypt returns delegate result on success`() {
    val delegateAead =
      mock<Aead> { on { encrypt(any(), anyOrNull()) } doAnswer { it.getArgument(0) } }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val plaintext = "plaintext".toByteArray()
    val result = client.getAead(KEY_URI).encrypt(plaintext, null)

    assertThat(result).isEqualTo(plaintext)
  }

  @Test
  fun `decrypt returns delegate result on success`() {
    val delegateAead =
      mock<Aead> { on { decrypt(any(), anyOrNull()) } doAnswer { it.getArgument(0) } }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val ciphertext = "ciphertext".toByteArray()
    val result = client.getAead(KEY_URI).decrypt(ciphertext, null)

    assertThat(result).isEqualTo(ciphertext)
  }

  @Test
  fun `encrypt translates a CompletionException into GeneralSecurityException`() {
    val credentialFailure = GeneralSecurityException("Failed to obtain AWS credentials")
    val delegateAead =
      mock<Aead> {
        on { encrypt(any(), anyOrNull()) } doThrow CompletionException(credentialFailure)
      }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<GeneralSecurityException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).hasCauseThat().isEqualTo(credentialFailure)
  }

  @Test
  fun `decrypt translates a CompletionException into GeneralSecurityException`() {
    val credentialFailure = GeneralSecurityException("Failed to obtain AWS credentials")
    val delegateAead =
      mock<Aead> {
        on { decrypt(any(), anyOrNull()) } doThrow CompletionException(credentialFailure)
      }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<GeneralSecurityException> {
        client.getAead(KEY_URI).decrypt("ciphertext".toByteArray(), null)
      }

    assertThat(exception).hasCauseThat().isEqualTo(credentialFailure)
  }

  @Test
  fun `encrypt does not double-wrap a GeneralSecurityException`() {
    val original = GeneralSecurityException("encryption failed")
    val delegateAead = mock<Aead> { on { encrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<GeneralSecurityException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `encrypt does not translate a plain RuntimeException`() {
    val original = RuntimeException("unrelated bug")
    val delegateAead = mock<Aead> { on { encrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<RuntimeException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `encrypt does not translate a CompletionException wrapping a RuntimeException`() {
    val original = CompletionException(IllegalStateException("bug"))
    val delegateAead = mock<Aead> { on { encrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<CompletionException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `decrypt does not translate a CompletionException wrapping a RuntimeException`() {
    val original = CompletionException(IllegalStateException("bug"))
    val delegateAead = mock<Aead> { on { decrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<CompletionException> {
        client.getAead(KEY_URI).decrypt("ciphertext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `encrypt does not translate a CompletionException wrapping an Error`() {
    val original = CompletionException(OutOfMemoryError("fatal"))
    val delegateAead = mock<Aead> { on { encrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<CompletionException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `decrypt does not translate a CompletionException wrapping an Error`() {
    val original = CompletionException(OutOfMemoryError("fatal"))
    val delegateAead = mock<Aead> { on { decrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<CompletionException> {
        client.getAead(KEY_URI).decrypt("ciphertext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `encrypt does not translate a CompletionException wrapping an unrelated checked exception`() {
    val original = CompletionException(java.io.IOException("unrelated"))
    val delegateAead = mock<Aead> { on { encrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<CompletionException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `encrypt does not translate a causeless CompletionException`() {
    val original = CompletionException("no cause", null)
    val delegateAead = mock<Aead> { on { encrypt(any(), anyOrNull()) } doThrow original }
    val delegateKmsClient = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception =
      assertFailsWith<CompletionException> {
        client.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
      }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `getAead propagates a delegate exception unchanged`() {
    val original = IllegalArgumentException("invalid key URI")
    val delegateKmsClient = mock<KmsClient> { on { getAead(any()) } doThrow original }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val exception = assertFailsWith<IllegalArgumentException> { client.getAead(KEY_URI) }

    assertThat(exception).isSameInstanceAs(original)
  }

  @Test
  fun `withCredentials preserves the exception translation`() {
    val delegateAead =
      mock<Aead> {
        on { encrypt(any(), anyOrNull()) } doThrow
          CompletionException(GeneralSecurityException("Failed to obtain AWS credentials"))
      }
    val credentialedDelegate = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val delegateKmsClient =
      mock<KmsClient> { on { withCredentials("/path") } doAnswer { credentialedDelegate } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val credentialedClient = client.withCredentials("/path")

    assertFailsWith<GeneralSecurityException> {
      credentialedClient.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
    }
  }

  @Test
  fun `withDefaultCredentials preserves the exception translation`() {
    val delegateAead =
      mock<Aead> {
        on { encrypt(any(), anyOrNull()) } doThrow
          CompletionException(GeneralSecurityException("Failed to obtain AWS credentials"))
      }
    val credentialedDelegate = mock<KmsClient> { on { getAead(KEY_URI) } doAnswer { delegateAead } }
    val delegateKmsClient =
      mock<KmsClient> { on { withDefaultCredentials() } doAnswer { credentialedDelegate } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    val credentialedClient = client.withDefaultCredentials()

    assertFailsWith<GeneralSecurityException> {
      credentialedClient.getAead(KEY_URI).encrypt("plaintext".toByteArray(), null)
    }
  }

  @Test
  fun `doesSupport delegates to the wrapped client`() {
    val delegateKmsClient = mock<KmsClient> { on { doesSupport(KEY_URI) } doAnswer { true } }
    val client = CompletionExceptionTranslatingKmsClient(delegateKmsClient)

    assertThat(client.doesSupport(KEY_URI)).isTrue()
  }
}

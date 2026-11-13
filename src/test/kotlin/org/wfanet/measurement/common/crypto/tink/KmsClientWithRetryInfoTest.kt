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

package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.subtle.AesGcmJce
import java.io.IOException
import java.security.GeneralSecurityException
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.common.crypto.tink.internal.KmsClientWithRetryInfo

class KmsClientWithRetryInfoTest {
  private val aead = AesGcmJce(ByteArray(32) { it.toByte() })

  @Test
  fun `getAead preserves encryption and associated data`() {
    val wrapped = KmsClientWithRetryInfo(client(aead)).getAead("test-key")
    val plaintext = "plaintext".toByteArray()
    for (associatedData in listOf(null, ByteArray(0), "context".toByteArray())) {
      val ciphertext = wrapped.encrypt(plaintext, associatedData)
      assertThat(aead.decrypt(ciphertext, associatedData)).isEqualTo(plaintext)
      assertThat(wrapped.decrypt(aead.encrypt(plaintext, associatedData), associatedData))
        .isEqualTo(plaintext)
    }
  }

  @Test
  fun `decrypt preserves invalid ciphertext errors`() {
    val wrapped = KmsClientWithRetryInfo(client(aead)).getAead("test-key")
    assertFailsWith<GeneralSecurityException> {
      wrapped.decrypt("invalid ciphertext".toByteArray(), null)
    }
  }

  @Test
  fun `encrypt preserves an adapter supplied retryable failure`() {
    val failure = KmsKeyAccessException("temporary", true, IOException("quota"))
    val wrapped = KmsClientWithRetryInfo(client(failingAead(failure))).getAead("test-key")
    assertThat(assertFailsWith<KmsKeyAccessException> { wrapped.encrypt(ByteArray(0), null) })
      .isSameInstanceAs(failure)
  }

  @Test
  fun `decrypt preserves an adapter supplied permanent failure`() {
    val failure = KmsKeyAccessException("permission denied", false)
    val wrapped = KmsClientWithRetryInfo(client(failingAead(failure))).getAead("test-key")
    assertThat(assertFailsWith<KmsKeyAccessException> { wrapped.decrypt(ByteArray(0), null) })
      .isSameInstanceAs(failure)
  }

  @Test
  fun `encrypt does not interpret an opaque failure cause chain`() {
    val original = GeneralSecurityException("opaque", IOException("hidden transport error"))
    val wrapped = KmsClientWithRetryInfo(client(failingAead(original))).getAead("test-key")
    val failure = assertFailsWith<GeneralSecurityException> { wrapped.encrypt(ByteArray(0), null) }
    assertThat(failure).isSameInstanceAs(original)
  }

  @Test
  fun `getAead preserves key lookup failure`() {
    val original = GeneralSecurityException("unsupported key")
    val delegate =
      object : KmsClient by client(aead) {
        override fun getAead(keyUri: String?): Aead = throw original
      }
    val failure =
      assertFailsWith<GeneralSecurityException> {
        KmsClientWithRetryInfo(delegate).getAead("unsupported-key")
      }
    assertThat(failure).isSameInstanceAs(original)
  }

  @Test
  fun `encrypt refreshes credentials on use and recovers after a temporary failure`() {
    var refreshes = 0
    val original = KmsKeyAccessException("refresh failed", true)
    val wrapped =
      KmsClientWithRetryInfo(client(aead)) {
          refreshes++
          KmsClientWithRetryInfo.awaitCredentials(
            if (refreshes == 1) CompletableFuture.failedFuture<Unit>(original)
            else CompletableFuture.completedFuture(Unit)
          )
        }
        .getAead("test-key")
    assertThat(refreshes).isEqualTo(0)
    assertThat(assertFailsWith<KmsKeyAccessException> { wrapped.encrypt(ByteArray(0), null) })
      .isSameInstanceAs(original)
    val ciphertext = wrapped.encrypt("plaintext".toByteArray(), null)
    assertThat(aead.decrypt(ciphertext, null)).isEqualTo("plaintext".toByteArray())
    assertThat(refreshes).isEqualTo(2)
  }

  @Test
  fun `withCredentials uses replacement credentials without refreshing the old credentials`() {
    val wrapped =
      KmsClientWithRetryInfo(client(aead)) { throw KmsKeyAccessException("old credentials", true) }
        .withCredentials("replacement")
        .getAead("test-key")
    val ciphertext = wrapped.encrypt("plaintext".toByteArray(), null)
    assertThat(aead.decrypt(ciphertext, null)).isEqualTo("plaintext".toByteArray())
  }

  @Test
  fun `credential preparation preserves opaque failures without inspecting causes`() {
    val original = GeneralSecurityException("opaque", IOException("hidden transport error"))
    val wrapped =
      KmsClientWithRetryInfo(client(aead)) {
          KmsClientWithRetryInfo.awaitCredentials(CompletableFuture.failedFuture<Unit>(original))
        }
        .getAead("test-key")
    val failure = assertFailsWith<GeneralSecurityException> { wrapped.encrypt(ByteArray(0), null) }
    assertThat(failure).isSameInstanceAs(original)
  }

  @Test
  fun `credential preparation restores the interrupt flag`() {
    val wrapped =
      KmsClientWithRetryInfo(client(aead)) {
          KmsClientWithRetryInfo.awaitCredentials(CompletableFuture<Unit>())
        }
        .getAead("test-key")
    try {
      Thread.currentThread().interrupt()
      val failure =
        assertFailsWith<GeneralSecurityException> { wrapped.encrypt(ByteArray(0), null) }
      assertThat(failure.cause).isInstanceOf(InterruptedException::class.java)
      assertThat(Thread.currentThread().isInterrupted).isTrue()
    } finally {
      Thread.interrupted()
    }
  }

  @Test
  fun `credential preparation preserves cancellation`() {
    val credentials = CompletableFuture<Unit>()
    credentials.cancel(false)
    val wrapped =
      KmsClientWithRetryInfo(client(aead)) { KmsClientWithRetryInfo.awaitCredentials(credentials) }
        .getAead("test-key")
    assertFailsWith<CancellationException> { wrapped.encrypt(ByteArray(0), null) }
  }

  private fun client(aead: Aead): KmsClient =
    object : KmsClient {
      override fun doesSupport(keyUri: String?) = keyUri == "test-key"

      override fun getAead(keyUri: String?) = aead

      override fun withCredentials(credentialPath: String?): KmsClient = client(aead)

      override fun withDefaultCredentials(): KmsClient = client(aead)
    }

  private fun failingAead(failure: GeneralSecurityException): Aead =
    object : Aead {
      override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray =
        throw failure

      override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray =
        throw failure
    }
}

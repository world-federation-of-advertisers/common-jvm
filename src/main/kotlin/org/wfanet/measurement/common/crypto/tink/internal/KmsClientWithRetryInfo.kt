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

package org.wfanet.measurement.common.crypto.tink.internal

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import java.security.GeneralSecurityException
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

/**
 * [KmsClient] wrapper preparing credentials before key use.
 *
 * Prepares credentials before entering the SDK, preserving retry information supplied by the
 * adapter. Key-operation errors pass through unchanged.
 *
 * @param delegate Client providing the cryptographic operations.
 * @param prepareKeyAccess Optional credential refresh performed before encryption or decryption.
 *   Replacing credentials through [withCredentials] or [withDefaultCredentials] drops this hook.
 */
internal class KmsClientWithRetryInfo(
  private val delegate: KmsClient,
  private val prepareKeyAccess: () -> Unit = {},
) : KmsClient by delegate {
  override fun withCredentials(credentialPath: String?): KmsClient =
    KmsClientWithRetryInfo(delegate.withCredentials(credentialPath))

  override fun withDefaultCredentials(): KmsClient =
    KmsClientWithRetryInfo(delegate.withDefaultCredentials())

  override fun getAead(keyUri: String?): Aead {
    val aead = delegate.getAead(keyUri)
    return object : Aead {
      override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray {
        prepareKeyAccess()
        return aead.encrypt(plaintext, associatedData)
      }

      override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray {
        prepareKeyAccess()
        return aead.decrypt(ciphertext, associatedData)
      }
    }
  }

  internal companion object {
    /** HTTP statuses for which another attempt can reasonably succeed after backoff. */
    @JvmSynthetic
    fun isRetryableHttpStatus(statusCode: Int): Boolean =
      when (statusCode) {
        408,
        429,
        500,
        502,
        503,
        504 -> true
        else -> false
      }

    /**
     * Resolves credentials before the SDK can replace the adapter's retry information.
     *
     * Uses [CompletableFuture.get]'s documented [ExecutionException] cause, without interpreting
     * SDK exceptions. The SDK subsequently uses the same provider's cached credentials.
     */
    @JvmSynthetic
    fun awaitCredentials(credentials: CompletableFuture<*>) {
      try {
        credentials.get()
      } catch (e: ExecutionException) {
        val cause = e.cause ?: e
        when (cause) {
          is GeneralSecurityException -> throw cause
          is CancellationException -> throw cause
          is Error -> throw cause
          else -> throw GeneralSecurityException("Failed to obtain KMS credentials", cause)
        }
      } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        throw GeneralSecurityException("Interrupted while obtaining KMS credentials", e)
      }
    }
  }
}

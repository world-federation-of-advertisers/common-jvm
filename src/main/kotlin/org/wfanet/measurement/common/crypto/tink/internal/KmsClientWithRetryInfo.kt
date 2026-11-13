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
import org.wfanet.measurement.common.crypto.tink.KmsKeyAccessException

/**
 * [KmsClient] wrapper exposing failures as [KmsKeyAccessException].
 *
 * Preserves retry information supplied by the adapter. Unclassified [GeneralSecurityException]s are
 * reported as nonretryable; their cause chains are not interpreted.
 *
 * @param delegate Client providing the cryptographic operations.
 * @param prepareKeyAccess Optional credential refresh performed before encryption or decryption.
 *   Replacing credentials through [withCredentials] or [withDefaultCredentials] drops this hook.
 */
internal class KmsClientWithRetryInfo(
  private val delegate: KmsClient,
  private val prepareKeyAccess: () -> Unit = {},
) : KmsClient by delegate {
  override fun withCredentials(credentialPath: String?): KmsClient = keyAccessCall {
    KmsClientWithRetryInfo(delegate.withCredentials(credentialPath))
  }

  override fun withDefaultCredentials(): KmsClient = keyAccessCall {
    KmsClientWithRetryInfo(delegate.withDefaultCredentials())
  }

  override fun getAead(keyUri: String?): Aead {
    val aead = keyAccessCall { delegate.getAead(keyUri) }
    return object : Aead {
      override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray =
        keyAccessCall {
          prepareKeyAccess()
          aead.encrypt(plaintext, associatedData)
        }

      override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray =
        keyAccessCall {
          prepareKeyAccess()
          aead.decrypt(ciphertext, associatedData)
        }
    }
  }

  private inline fun <T> keyAccessCall(block: () -> T): T =
    try {
      block()
    } catch (e: KmsKeyAccessException) {
      throw e
    } catch (e: GeneralSecurityException) {
      throw KmsKeyAccessException("KMS key access failed", isRetryable = false, cause = e)
    }
}

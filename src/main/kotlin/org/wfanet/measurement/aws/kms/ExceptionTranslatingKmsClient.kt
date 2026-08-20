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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import java.security.GeneralSecurityException
import java.util.concurrent.CompletionException

/**
 * Wraps [delegate] so [getAead] and the [Aead] operations it returns only ever throw
 * [GeneralSecurityException] — the complete failure contract each declares.
 */
class ExceptionTranslatingKmsClient(private val delegate: KmsClient) : KmsClient by delegate {
  override fun withCredentials(credentialPath: String?): KmsClient =
    ExceptionTranslatingKmsClient(delegate.withCredentials(credentialPath))

  override fun withDefaultCredentials(): KmsClient =
    ExceptionTranslatingKmsClient(delegate.withDefaultCredentials())

  override fun getAead(keyUri: String?): Aead {
    // Upstream's own getAead() throws IllegalArgumentException for a malformed key URI rather
    // than GeneralSecurityException (its internal catch only covers SdkClientException).
    val delegateAead =
      try {
        delegate.getAead(keyUri)
      } catch (e: IllegalArgumentException) {
        throw GeneralSecurityException("Invalid AWS KMS key URI: $keyUri", e)
      }
    return object : Aead {
      override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray =
        inKmsCall {
          delegateAead.encrypt(plaintext, associatedData)
        }

      override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray =
        inKmsCall {
          delegateAead.decrypt(ciphertext, associatedData)
        }

      /**
       * Runs [block], translating [CompletionException] into [GeneralSecurityException].
       *
       * A failed AWS credential refresh can surface here as a raw [CompletionException] instead of
       * the [GeneralSecurityException] upstream's own catch clause expects (it only catches
       * `SdkClientException`/`KmsException`): the AWS SDK's internal synchronous credential
       * resolution unwraps a failed future's [CompletionException] down to its cause only when that
       * cause is a [RuntimeException], and a credential chain backed by
       * [org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider] can fail with a checked
       * [GeneralSecurityException] instead, which the SDK leaves wrapped.
       *
       * Filed upstream as https://github.com/tink-crypto/tink-java-awskms/issues/5 (fix pending
       * as https://github.com/tink-crypto/tink-java-awskms/pull/7); this can go away once a
       * release including that fix is available.
       */
      private inline fun <T> inKmsCall(block: () -> T): T =
        try {
          block()
        } catch (e: CompletionException) {
          throw GeneralSecurityException("AWS KMS call failed", e.cause ?: e)
        }
    }
  }
}

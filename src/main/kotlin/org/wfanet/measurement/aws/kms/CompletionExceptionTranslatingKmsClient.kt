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
 * Wraps [delegate] so the [Aead] returned by [getAead] unwraps a [CompletionException] from
 * `encrypt`/`decrypt` into its cause.
 *
 * A failed async credential refresh (e.g. a credential chain backed by
 * [org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider]) surfaces here as a raw
 * [CompletionException]: the AWS SDK's internal synchronous credential resolution unwraps a failed
 * future's `CompletionException` down to its cause only when that cause is a [RuntimeException],
 * and such a credential chain can fail with a checked [GeneralSecurityException] instead, which the
 * SDK leaves wrapped.
 *
 * TODO(tink-crypto/tink-java-awskms#5): drop this class once a release including the fix
 *   (tink-crypto/tink-java-awskms#7) is available.
 */
class CompletionExceptionTranslatingKmsClient(private val delegate: KmsClient) :
  KmsClient by delegate {
  override fun withCredentials(credentialPath: String?): KmsClient =
    CompletionExceptionTranslatingKmsClient(delegate.withCredentials(credentialPath))

  override fun withDefaultCredentials(): KmsClient =
    CompletionExceptionTranslatingKmsClient(delegate.withDefaultCredentials())

  override fun getAead(keyUri: String?): Aead {
    val delegateAead = delegate.getAead(keyUri)
    return object : Aead {
      override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray =
        inKmsCall {
          delegateAead.encrypt(plaintext, associatedData)
        }

      override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray =
        inKmsCall {
          delegateAead.decrypt(ciphertext, associatedData)
        }

      private inline fun <T> inKmsCall(block: () -> T): T =
        try {
          block()
        } catch (e: CompletionException) {
          throw GeneralSecurityException("KMS operation failed", e.cause ?: e)
        }
    }
  }
}

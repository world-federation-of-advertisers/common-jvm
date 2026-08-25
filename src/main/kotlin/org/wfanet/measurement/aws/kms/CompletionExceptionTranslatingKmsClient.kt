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
import com.google.crypto.tink.integration.awskms.AwsKmsClient as TinkAwsKmsClient
import java.security.GeneralSecurityException
import java.util.concurrent.CompletionException

/**
 * Wraps [delegate] so the [Aead] returned by [getAead] translates the one specific checked-failure
 * shape AWS credential resolution can leave wrapped in a [CompletionException] from
 * `encrypt`/`decrypt` into a [GeneralSecurityException].
 *
 * A failed async credential refresh (e.g. a credential chain backed by
 * [org.wfanet.measurement.aws.RefreshableAwsCredentialsIdentityProvider]) surfaces here as a raw
 * [CompletionException]: the AWS SDK's internal synchronous credential resolution unwraps a failed
 * future's `CompletionException` down to its cause only when that cause is a [RuntimeException],
 * and such a credential chain can fail with a checked [GeneralSecurityException] instead, which the
 * SDK leaves wrapped. Only a [GeneralSecurityException] cause is translated; anything else --
 * `null`, a [RuntimeException], an [Error], or some other checked exception type this class has no
 * specific knowledge of -- is not that documented shape, so the [CompletionException] itself is
 * rethrown unchanged rather than being unwrapped or otherwise guessed at.
 *
 * This is a workaround specifically for [TinkAwsKmsClient]'s behavior, not a general-purpose
 * [KmsClient] wrapper -- another implementation's [CompletionException]s, if any, would not
 * necessarily share this shape.
 *
 * TODO(tink-crypto/tink-java-awskms#5): drop this class once a release including the fix is
 *   available.
 */
class CompletionExceptionTranslatingKmsClient(private val delegate: TinkAwsKmsClient) :
  KmsClient by delegate {
  override fun withCredentials(credentialPath: String?): KmsClient =
    wrap(delegate.withCredentials(credentialPath))

  override fun withDefaultCredentials(): KmsClient = wrap(delegate.withDefaultCredentials())

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
          val cause = e.cause
          if (cause !is GeneralSecurityException) {
            throw e
          }
          throw GeneralSecurityException("KMS operation failed", cause)
        }
    }
  }

  companion object {
    /**
     * Wraps [kmsClient], which must be a [TinkAwsKmsClient] -- the only concrete type
     * [TinkAwsKmsClient]'s own [KmsClient]-typed methods
     * ([TinkAwsKmsClient.withCredentialsProvider], [TinkAwsKmsClient.withCredentials],
     * [TinkAwsKmsClient.withDefaultCredentials]) ever return.
     */
    fun wrap(kmsClient: KmsClient): CompletionExceptionTranslatingKmsClient {
      require(kmsClient is TinkAwsKmsClient) {
        "Expected a TinkAwsKmsClient, got ${kmsClient::class}"
      }
      return CompletionExceptionTranslatingKmsClient(kmsClient)
    }
  }
}

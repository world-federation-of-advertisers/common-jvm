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
       * Runs [block], translating any exception other than [GeneralSecurityException] into one.
       *
       * Credential resolution failures can surface here in shapes upstream's own catch clause
       * doesn't expect (it only catches `SdkClientException`/`KmsException`), because they
       * originate below the AWS SDK's own exception mapping:
       * - A failed async credential refresh (e.g. a credential chain backed by
       *   [org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider]) surfaces as a raw
       *   [java.util.concurrent.CompletionException]: the AWS SDK's internal synchronous credential
       *   resolution unwraps a failed future's `CompletionException` down to its cause only when
       *   that cause is a [RuntimeException], and such a credential chain can fail with a checked
       *   [GeneralSecurityException] instead, which the SDK leaves wrapped.
       * - A synchronous credential provider (e.g. the AWS SDK's own
       *   `StsWebIdentityTokenFileCredentialsProvider`) can throw directly out of
       *   `AwsCredentialsProvider`'s default `resolveIdentity`, which calls `resolveCredentials`
       *   eagerly rather than deferring it into the future it returns -- surfacing, for example, as
       *   a raw `UncheckedIOException` when a configured token file doesn't exist.
       *
       * Rather than enumerate every such shape, this catches any [Exception] not already a
       * [GeneralSecurityException] and translates it, so [Aead.encrypt]/[Aead.decrypt] honor their
       * documented contract regardless of how the underlying credential provider fails.
       *
       * Filed upstream as https://github.com/tink-crypto/tink-java-awskms/issues/5 (fix pending as
       * https://github.com/tink-crypto/tink-java-awskms/pull/7); this can go away once a release
       * including that fix is available.
       */
      private inline fun <T> inKmsCall(block: () -> T): T =
        try {
          block()
        } catch (e: GeneralSecurityException) {
          throw e
        } catch (e: CompletionException) {
          throw GeneralSecurityException("AWS KMS call failed", e.cause ?: e)
        } catch (e: Exception) {
          throw GeneralSecurityException("AWS KMS call failed", e)
        }
    }
  }
}

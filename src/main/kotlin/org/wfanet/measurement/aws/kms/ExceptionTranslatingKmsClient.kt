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
 * Wraps [delegate], a [KmsClient] whose own [KmsClient.getAead] can throw
 * [IllegalArgumentException] or [NullPointerException] instead of [GeneralSecurityException] for an
 * unsupported key URI (tink-crypto/tink-java-awskms#9, tink-crypto/tink-java-gcpkms#6), so that
 * [getAead] validates the URI itself first and never passes an unsupported one to [delegate].
 *
 * This does not attempt to translate every other exception [delegate] or the [Aead] it returns
 * might throw: this class has no way to know what any given [delegate] implementation does or
 * doesn't guarantee beyond the [KmsClient] interface, so guessing at translations for arbitrary
 * runtime exceptions risks masking real bugs instead of exposing them.
 */
class ExceptionTranslatingKmsClient(private val delegate: KmsClient) : KmsClient by delegate {
  override fun withCredentials(credentialPath: String?): KmsClient =
    ExceptionTranslatingKmsClient(delegate.withCredentials(credentialPath))

  override fun withDefaultCredentials(): KmsClient =
    ExceptionTranslatingKmsClient(delegate.withDefaultCredentials())

  override fun getAead(keyUri: String?): Aead {
    if (keyUri == null || !delegate.doesSupport(keyUri) || !hasValidKeyArn(keyUri)) {
      throw GeneralSecurityException("Invalid AWS KMS key URI: $keyUri")
    }
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

      /**
       * Runs [block], unwrapping a [CompletionException] into its cause.
       *
       * A failed async credential refresh (e.g. a credential chain backed by
       * [org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider]) surfaces here as a raw
       * [CompletionException]: the AWS SDK's internal synchronous credential resolution unwraps a
       * failed future's `CompletionException` down to its cause only when that cause is a
       * [RuntimeException], and such a credential chain can fail with a checked
       * [GeneralSecurityException] instead, which the SDK leaves wrapped.
       *
       * TODO(tink-crypto/tink-java-awskms#5): drop this catch once a release including the fix
       *   (tink-crypto/tink-java-awskms#7) is available.
       */
      private inline fun <T> inKmsCall(block: () -> T): T =
        try {
          block()
        } catch (e: GeneralSecurityException) {
          throw e
        } catch (e: CompletionException) {
          throw GeneralSecurityException("AWS KMS call failed", e.cause ?: e)
        }
    }
  }

  /**
   * Returns whether [keyUri] has the ARN structure upstream's `getAead` requires beyond the
   * `aws-kms://` prefix [KmsClient.doesSupport] already checks: at least 4 colon-separated segments
   * after the prefix, matching an ARN's `partition:service:region:account-id:resource`.
   *
   * Upstream's own `doesSupport` doesn't check this, so it can return true for a URI whose
   * `getAead` then throws `IllegalArgumentException` instead of [GeneralSecurityException]
   * (tink-crypto/tink-java-awskms#9).
   */
  private fun hasValidKeyArn(keyUri: String): Boolean =
    keyUri.substring(AWS_KMS_PREFIX.length).split(':').size >= 4

  companion object {
    private const val AWS_KMS_PREFIX = "aws-kms://"
  }
}

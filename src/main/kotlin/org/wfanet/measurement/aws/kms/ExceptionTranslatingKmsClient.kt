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
 * Wraps [delegate], translating [CompletionException] thrown by its [Aead] operations into
 * [GeneralSecurityException].
 *
 * [Aead.encrypt] and [Aead.decrypt] are declared to throw only [GeneralSecurityException] — that is
 * the entire failure contract Tink promises, and callers of AWS KMS-backed [Aead]s elsewhere in
 * this codebase (e.g. TrusTeeMill in cross-media-measurement) are written against exactly that
 * contract. The upstream `tink-awskms` client's own [Aead] implementation
 * (`com.google.crypto.tink.integration.awskms.AwsKmsAead`) only catches the AWS SDK's own
 * `SdkClientException` and `KmsException` when translating failures into [GeneralSecurityException]
 * — it has no way to know about failure modes introduced by credential sources it didn't author.
 *
 * That gap matters specifically for
 * [org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider]-backed credentials, used by
 * [org.wfanet.measurement.gcloud.kms.GCloudToAwsKmsClientFactory] and
 * [org.wfanet.measurement.gcloud.kms.ConfidentialSpaceToAwsKmsClientFactory]: their credential
 * chains can fail with a checked [GeneralSecurityException], which is NOT a [RuntimeException]. The
 * AWS SDK's internal synchronous credential resolution (`AwsCredentialsAuthorizationStrategy`, via
 * `CompletableFutureUtils.joinLikeSync`) only unwraps a failed future's [CompletionException] down
 * to its cause when that cause is a [RuntimeException]; since [GeneralSecurityException] is not
 * one, the raw [CompletionException] escapes instead. Upstream's narrow catch clause doesn't
 * recognize [CompletionException], so it propagates uncaught straight past the [Aead] boundary —
 * silently breaking the exception contract callers rely on.
 *
 * The deprecated custom `AwsKmsClient` never had this gap: its own `inKmsCall` helper already
 * caught [CompletionException] alongside the AWS SDK exception types. This class restores the same
 * behavior for the upstream client, so callers see identical exception behavior regardless of which
 * client a [org.wfanet.measurement.common.crypto.tink.KmsClientFactory] happens to return.
 *
 * Only [KmsClient.getAead]'s returned [Aead] is wrapped. [KmsClient]'s other methods
 * ([KmsClient.doesSupport], [KmsClient.withCredentials], [KmsClient.withDefaultCredentials]) don't
 * perform credential resolution, so they're delegated to [delegate] unchanged.
 */
class ExceptionTranslatingKmsClient(private val delegate: KmsClient) : KmsClient by delegate {
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

      /**
       * Runs [block], translating a [CompletionException] — the shape a failed AWS credential
       * refresh takes once it escapes the AWS SDK's internal join (see the class-level doc) — into
       * [GeneralSecurityException], matching what [Aead] callers expect.
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

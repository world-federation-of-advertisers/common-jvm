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

package org.wfanet.measurement.aws

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest

/**
 * Adapts an [IdentityProvider] of [AwsCredentialsIdentity] to the [AwsCredentialsProvider] type
 * some AWS SDK integrations require in place of the more general [IdentityProvider] -- for
 * integrations that only ever call [resolveIdentity]. [resolveCredentials] is not supported; see
 * its KDoc.
 *
 * Checked failures from the delegate are translated to [SdkClientException] so synchronous AWS
 * clients surface them through their documented credential-loading failure path.
 */
class AwsCredentialsProviderAdapter(
  private val delegate: IdentityProvider<AwsCredentialsIdentity>
) : AwsCredentialsProvider {

  override fun resolveIdentity(
    request: ResolveIdentityRequest
  ): CompletableFuture<AwsCredentialsIdentity> =
    delegate
      .resolveIdentity(request)
      .thenApply { it }
      .exceptionally { exception -> throw normalizeFailure(exception) }

  private fun normalizeFailure(exception: Throwable): Throwable {
    val cause: Throwable =
      if (exception is CompletionException && exception.cause != null) {
        exception.cause!!
      } else {
        exception
      }
    return when (cause) {
      is RuntimeException,
      is Error -> cause
      else -> SdkClientException.create("Failed to resolve AWS credentials", cause)
    }
  }

  /**
   * Returns [AwsCredentials] that can be used to authorize an AWS request.
   *
   * Unsupported by this implementation. Use [resolveIdentity] to resolve credentials
   * asynchronously.
   *
   * @throws UnsupportedOperationException unconditionally
   */
  override fun resolveCredentials(): AwsCredentials =
    throw UnsupportedOperationException("Use resolveIdentity to resolve credentials asynchronously")
}

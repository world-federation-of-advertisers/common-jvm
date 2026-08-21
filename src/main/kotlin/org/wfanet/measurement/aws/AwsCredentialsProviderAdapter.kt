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
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest

/**
 * Adapts an [IdentityProvider] of [AwsCredentialsIdentity] to the synchronous
 * [AwsCredentialsProvider] interface some AWS SDK integrations still require in place of the more
 * general [IdentityProvider].
 */
class AwsCredentialsProviderAdapter(
  private val delegate: IdentityProvider<AwsCredentialsIdentity>
) : AwsCredentialsProvider {

  override fun resolveIdentity(
    request: ResolveIdentityRequest
  ): CompletableFuture<AwsCredentialsIdentity> =
    // .thenApply { it } adapts the Java wildcard return type
    // (CompletableFuture<? extends AwsCredentialsIdentity>) to the invariant type Kotlin requires
    // for this override's signature.
    delegate.resolveIdentity(request).thenApply { it }

  /**
   * Throws [UnsupportedOperationException] because credentials must be resolved through
   * [resolveIdentity].
   */
  override fun resolveCredentials(): AwsCredentials =
    throw UnsupportedOperationException("Use resolveIdentity to resolve credentials asynchronously")
}

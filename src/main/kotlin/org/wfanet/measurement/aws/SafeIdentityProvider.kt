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

import java.security.GeneralSecurityException
import java.util.concurrent.CompletableFuture
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest

/**
 * Wraps [delegate] so a synchronous credential-resolution failure completes the returned
 * [CompletableFuture] exceptionally with a [GeneralSecurityException], instead of throwing directly
 * out of [resolveIdentity].
 *
 * Some [IdentityProvider]s resolve synchronously under the hood -- e.g. the AWS SDK's own
 * `StsWebIdentityTokenFileCredentialsProvider`, an `AwsCredentialsProvider` whose inherited default
 * `resolveIdentity` calls `resolveCredentials` eagerly -- so a failure there (such as a missing web
 * identity token file) throws synchronously rather than failing the future [resolveIdentity] is
 * supposed to return.
 */
class SafeIdentityProvider(private val delegate: IdentityProvider<AwsCredentialsIdentity>) :
  IdentityProvider<AwsCredentialsIdentity> by delegate {

  override fun resolveIdentity(
    request: ResolveIdentityRequest
  ): CompletableFuture<AwsCredentialsIdentity> =
    try {
      // .thenApply { it } adapts the Java wildcard return type
      // (CompletableFuture<? extends AwsCredentialsIdentity>) to the invariant type Kotlin
      // requires for this override's signature.
      delegate.resolveIdentity(request).thenApply { it }
    } catch (e: Exception) {
      CompletableFuture.failedFuture(
        GeneralSecurityException("Failed to resolve AWS credentials", e)
      )
    }
}

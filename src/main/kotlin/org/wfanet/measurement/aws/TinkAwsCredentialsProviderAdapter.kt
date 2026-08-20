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
 * [AwsCredentialsProvider] interface some third-party libraries built on the AWS SDK require:
 * notably upstream `tink-awskms`'s
 * `com.google.crypto.tink.integration.awskms.AwsKmsClient.withCredentialsProvider`, whose public
 * API has no [IdentityProvider]-based overload.
 *
 * [AwsCredentialsProvider] declares a default [resolveIdentity] that delegates to
 * [resolveCredentials], so an [AwsCredentialsProvider] whose credential source is inherently
 * asynchronous cannot just implement [resolveCredentials] and inherit that default — doing so would
 * route every [resolveIdentity] call through the blocking method it can't correctly implement. This
 * adapter instead forwards [resolveIdentity] directly to [delegate], and [resolveCredentials]
 * throws: nothing should call it, since the AWS SDK's own client builders and internal auth
 * strategy
 * (`software.amazon.awssdk.awscore.internal.authcontext.AwsCredentialsAuthorizationStrategy`) only
 * ever call [resolveIdentity], joining on the result when a synchronous value is ultimately
 * required.
 *
 * Filed upstream as https://github.com/tink-crypto/tink-java-awskms/issues/6 (fix pending as
 * https://github.com/tink-crypto/tink-java-awskms/pull/8); once a release including that fix is
 * available, callers can pass [delegate] directly and this adapter can be deleted.
 */
// TODO(tink-crypto/tink-java-awskms#6): remove this adapter once a release including the fix
// (tink-crypto/tink-java-awskms#8) is available; callers can then pass an IdentityProvider
// directly to withCredentialsProvider.
class TinkAwsCredentialsProviderAdapter(
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
   * Always throws: real callers resolve credentials through [resolveIdentity] instead (see the
   * class-level documentation). Throwing — rather than blocking on [resolveIdentity] — turns an
   * unexpected direct call into an immediate, obvious failure instead of a silent deadlock if the
   * caller happens to be on a non-blocking thread (e.g. a Reactor Netty event loop).
   */
  override fun resolveCredentials(): AwsCredentials =
    throw UnsupportedOperationException("Use resolveIdentity to resolve credentials asynchronously")
}

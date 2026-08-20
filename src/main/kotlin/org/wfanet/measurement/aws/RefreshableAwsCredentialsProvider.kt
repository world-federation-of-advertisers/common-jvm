// Copyright 2025 The Cross-Media Measurement Authors
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

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest

/**
 * AWS session credentials paired with their expiration time.
 *
 * @param credentials The AWS session credentials.
 * @param expiration The time at which these credentials expire.
 */
data class TimeBoundCredentials(val credentials: AwsSessionCredentials, val expiration: Instant)

/**
 * An [AwsCredentialsProvider] (and, transitively, [IdentityProvider] of [AwsCredentialsIdentity])
 * that caches temporary AWS session credentials and proactively refreshes them before they expire.
 *
 * Use this when AWS credentials are obtained through a multi-step exchange that the AWS SDK cannot
 * manage natively — for example, federated identity flows where a GCP Confidential Space
 * attestation token is exchanged for a Google Cloud access token, which is then exchanged via AWS
 * STS `AssumeRoleWithWebIdentity` for temporary AWS credentials. In these cases, the AWS SDK's
 * built-in credential providers (such as `StsAssumeRoleWithWebIdentityCredentialsProvider`) cannot
 * be used because the web identity token itself must be re-obtained through a separate provider
 * before each STS call.
 *
 * Credentials are obtained lazily on the first call to [resolveIdentity] and cached until they are
 * within [refreshMargin] of expiration, at which point [credentialSupplier] is called again. A
 * refresh that fails is not cached, so the next call retries.
 *
 * Thread-safe: callers that arrive while a refresh is in flight share its result rather than
 * starting a second one.
 *
 * Declares [AwsCredentialsProvider] — which is-a [IdentityProvider] of [AwsCredentialsIdentity] —
 * rather than [IdentityProvider] directly, solely to satisfy the *type* some third-party libraries
 * built on the AWS SDK require: notably upstream `tink-awskms`'s
 * `com.google.crypto.tink.integration.awskms.AwsKmsClient.withCredentialsProvider`, whose public
 * API has no [IdentityProvider]-based overload. This does not change how credentials are actually
 * resolved. [credentialSupplier] is inherently asynchronous, so [resolveIdentity] remains the real
 * entry point — including for the AWS SDK's own client builders and its internal auth strategy
 * (`software.amazon.awssdk.awscore.internal.authcontext.AwsCredentialsAuthorizationStrategy`),
 * which calls [resolveIdentity] and joins on the result only when a synchronous value is ultimately
 * required. [resolveCredentials] intentionally throws instead — see its documentation.
 *
 * Filed upstream as https://github.com/tink-crypto/tink-java-awskms/issues/6 (fix pending as
 * https://github.com/tink-crypto/tink-java-awskms/pull/8); once a release including that fix is
 * available, this class could drop the [AwsCredentialsProvider] supertype and [resolveCredentials]
 * entirely, implementing [IdentityProvider] directly.
 *
 * @param refreshMargin How far before expiration to proactively refresh credentials.
 * @param clock Clock used to determine the current time.
 * @param credentialSupplier Function that starts obtaining fresh credentials and their expiration.
 */
class RefreshableAwsCredentialsProvider(
  private val refreshMargin: Duration,
  private val clock: Clock = Clock.systemUTC(),
  private val credentialSupplier: () -> CompletableFuture<TimeBoundCredentials>,
) : AwsCredentialsProvider {

  @Volatile private var cachedCredentials: TimeBoundCredentials? = null

  /** Refresh that has been started but has not completed yet. Guarded by `this`. */
  private var inFlightRefresh: CompletableFuture<TimeBoundCredentials>? = null

  override fun resolveIdentity(
    request: ResolveIdentityRequest
  ): CompletableFuture<AwsCredentialsIdentity> {
    val current: TimeBoundCredentials? = cachedCredentials
    if (current != null && isCurrent(current)) {
      return CompletableFuture.completedFuture(current.credentials)
    }

    val refresh: CompletableFuture<TimeBoundCredentials> =
      synchronized(this) {
        val currentUnderLock: TimeBoundCredentials? = cachedCredentials
        if (currentUnderLock != null && isCurrent(currentUnderLock)) {
          return CompletableFuture.completedFuture(currentUnderLock.credentials)
        }
        inFlightRefresh ?: startRefresh()
      }
    return refresh.thenApply { it.credentials }
  }

  /**
   * Always throws: real callers resolve credentials through [resolveIdentity] instead (see the
   * class-level documentation). Throwing — rather than blocking on [resolveIdentity] — turns an
   * unexpected direct call into an immediate, obvious failure instead of a silent deadlock if the
   * caller happens to be on a non-blocking thread (e.g. a Reactor Netty event loop).
   */
  override fun resolveCredentials(): AwsCredentials =
    throw UnsupportedOperationException("Use resolveIdentity to resolve credentials asynchronously")

  private fun isCurrent(credentials: TimeBoundCredentials): Boolean =
    clock.instant().plus(refreshMargin).isBefore(credentials.expiration)

  /**
   * Starts a refresh and records it as the in-flight one, so that concurrent callers share it.
   *
   * Must be called while holding `this`.
   */
  private fun startRefresh(): CompletableFuture<TimeBoundCredentials> {
    logger.info("Refreshing AWS credentials")
    val refresh: CompletableFuture<TimeBoundCredentials> =
      try {
        credentialSupplier()
      } catch (e: Exception) {
        return CompletableFuture.failedFuture(e)
      }
    inFlightRefresh = refresh
    refresh.whenComplete { result, error ->
      synchronized(this) {
        // Only clear the in-flight refresh if it is still this one, otherwise a later refresh
        // would be dropped and its callers would each start their own.
        if (inFlightRefresh === refresh) {
          inFlightRefresh = null
        }
        if (error == null) {
          cachedCredentials = result
        }
      }
      if (error == null) {
        logger.info("AWS credentials refreshed, expiration: ${result.expiration}")
      }
    }
    return refresh
  }

  companion object {
    private val logger: Logger =
      Logger.getLogger(RefreshableAwsCredentialsProvider::class.java.name)
  }
}

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

package org.wfanet.measurement.gcloud.kms

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

/**
 * AWS session credentials paired with their expiration time.
 *
 * @param credentials The AWS session credentials.
 * @param expiration The time at which these credentials expire.
 */
data class TimeBoundCredentials(val credentials: AwsSessionCredentials, val expiration: Instant)

/**
 * An [AwsCredentialsProvider] that caches credentials and refreshes them when they are within
 * [refreshMargin] of expiration.
 *
 * Thread-safe: concurrent calls to [resolveCredentials] are serialized via double-checked locking
 * so that only one thread executes the credential supplier when a refresh is needed.
 *
 * @param refreshMargin How far before expiration to proactively refresh credentials.
 * @param clock Clock used to determine the current time.
 * @param credentialSupplier Function that obtains fresh credentials and their expiration.
 */
class RefreshableAwsCredentialsProvider(
  private val refreshMargin: Duration,
  private val clock: Clock = Clock.systemUTC(),
  private val credentialSupplier: () -> TimeBoundCredentials,
) : AwsCredentialsProvider {

  @Volatile private var cachedCredentials: AwsSessionCredentials? = null
  @Volatile private var expiration: Instant = Instant.EPOCH

  override fun resolveCredentials(): AwsCredentials {
    val now = clock.instant()
    val current = cachedCredentials
    if (current != null && now.plus(refreshMargin).isBefore(expiration)) {
      return current
    }

    synchronized(this) {
      val nowAfterLock = clock.instant()
      val currentAfterLock = cachedCredentials
      if (currentAfterLock != null && nowAfterLock.plus(refreshMargin).isBefore(expiration)) {
        return currentAfterLock
      }

      logger.info("Refreshing AWS credentials")
      val result = credentialSupplier()
      cachedCredentials = result.credentials
      expiration = result.expiration
      logger.info("AWS credentials refreshed, expiration: ${result.expiration}")
      return result.credentials
    }
  }

  companion object {
    private val logger: Logger =
      Logger.getLogger(RefreshableAwsCredentialsProvider::class.java.name)
  }
}

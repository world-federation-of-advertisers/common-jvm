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

package org.wfanet.measurement.aws.kms

import com.google.common.truth.Truth.assertThat
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

@RunWith(JUnit4::class)
class RefreshableAwsCredentialsProviderTest {

  @Test
  fun `obtains credentials on first call`() {
    var callCount = 0
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = Instant.parse("2026-06-03T13:00:00Z"),
        )
      }

    assertThat(callCount).isEqualTo(0)
    val result = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(result.accessKeyId()).isEqualTo("key-1")
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `returns cached credentials when well before expiry`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = Instant.parse("2026-06-03T13:00:00Z"),
        )
      }

    val first = provider.resolveCredentials()
    val second = provider.resolveCredentials()
    val third = provider.resolveCredentials()

    assertThat(second).isSameInstanceAs(first)
    assertThat(third).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `refreshes when current time is within refresh margin of expiry`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val withinMargin = expiration.minus(Duration.ofMinutes(3))
    val clock = Clock.fixed(withinMargin, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = if (callCount == 1) expiration else expiration.plusSeconds(3600),
        )
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `refreshes when credentials are already expired`() {
    val pastExpiration = Instant.parse("2026-06-03T11:00:00Z")
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = if (callCount == 1) pastExpiration else Instant.parse("2026-06-03T13:00:00Z"),
        )
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `does not refresh when outside refresh margin`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val outsideMargin = expiration.minus(Duration.ofMinutes(10))
    val clock = Clock.fixed(outsideMargin, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = expiration,
        )
      }

    val first = provider.resolveCredentials()
    val second = provider.resolveCredentials()

    assertThat(second).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `refreshes exactly at the margin boundary`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val exactlyAtMargin = expiration.minus(Duration.ofMinutes(5))
    val clock = Clock.fixed(exactlyAtMargin, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = if (callCount == 1) expiration else expiration.plusSeconds(3600),
        )
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `propagates supplier exception`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        throw RuntimeException("credential chain failed")
      }

    val exception = assertFailsWith<RuntimeException> { provider.resolveCredentials() }
    assertThat(exception).hasMessageThat().contains("credential chain failed")
  }

  @Test
  fun `retries after supplier failure on next call`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        if (callCount == 1) throw RuntimeException("transient failure")
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = Instant.parse("2026-06-03T13:00:00Z"),
        )
      }

    assertFailsWith<RuntimeException> { provider.resolveCredentials() }
    assertThat(callCount).isEqualTo(1)

    val result = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(result.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `zero refresh margin only refreshes when expired`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val justBeforeExpiry = expiration.minusSeconds(1)
    val clock = Clock.fixed(justBeforeExpiry, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ZERO, clock = clock) {
        callCount++
        TimeBoundCredentials(
          credentials = makeCredentials("key-$callCount"),
          expiration = expiration,
        )
      }

    val first = provider.resolveCredentials()
    val second = provider.resolveCredentials()

    assertThat(second).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `concurrent calls invoke supplier exactly once`() = runBlocking {
    val callCount = AtomicInteger(0)
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        Thread.sleep(100)
        val count = callCount.incrementAndGet()
        TimeBoundCredentials(
          credentials = makeCredentials("key-$count"),
          expiration = Instant.parse("2026-06-03T13:00:00Z"),
        )
      }

    val results =
      (1..50).map { async(Dispatchers.Default) { provider.resolveCredentials() } }.awaitAll()

    assertThat(callCount.get()).isEqualTo(1)
    val first = results.first()
    results.forEach { assertThat(it).isSameInstanceAs(first) }
  }

  private fun makeCredentials(accessKeyId: String): AwsSessionCredentials {
    return AwsSessionCredentials.create(accessKeyId, "secret", "token")
  }
}

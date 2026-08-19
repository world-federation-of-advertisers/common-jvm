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

import com.google.common.truth.Truth.assertThat
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity

@RunWith(JUnit4::class)
class RefreshableAwsCredentialsProviderTest {

  @Test
  fun `resolveIdentity obtains credentials on first call`() {
    var callCount = 0
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        completedRefresh("key-$callCount", Instant.parse("2026-06-03T13:00:00Z"))
      }

    assertThat(callCount).isEqualTo(0)
    val result = provider.resolveIdentity().get()
    assertThat(result.accessKeyId()).isEqualTo("key-1")
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `resolveIdentity returns cached credentials when well before expiry`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        completedRefresh("key-$callCount", Instant.parse("2026-06-03T13:00:00Z"))
      }

    val first = provider.resolveIdentity().get()
    val second = provider.resolveIdentity().get()
    val third = provider.resolveIdentity().get()

    assertThat(second).isSameInstanceAs(first)
    assertThat(third).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `resolveIdentity refreshes when current time is within refresh margin of expiry`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val withinMargin = expiration.minus(Duration.ofMinutes(3))
    val clock = Clock.fixed(withinMargin, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        completedRefresh(
          "key-$callCount",
          if (callCount == 1) expiration else expiration.plusSeconds(3600),
        )
      }

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-1")

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `resolveIdentity refreshes when credentials are already expired`() {
    val pastExpiration = Instant.parse("2026-06-03T11:00:00Z")
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        completedRefresh(
          "key-$callCount",
          if (callCount == 1) pastExpiration else Instant.parse("2026-06-03T13:00:00Z"),
        )
      }

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-1")

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `resolveIdentity does not refresh when outside refresh margin`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val outsideMargin = expiration.minus(Duration.ofMinutes(10))
    val clock = Clock.fixed(outsideMargin, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        completedRefresh("key-$callCount", expiration)
      }

    val first = provider.resolveIdentity().get()
    val second = provider.resolveIdentity().get()

    assertThat(second).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `resolveIdentity refreshes exactly at the margin boundary`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val exactlyAtMargin = expiration.minus(Duration.ofMinutes(5))
    val clock = Clock.fixed(exactlyAtMargin, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        completedRefresh(
          "key-$callCount",
          if (callCount == 1) expiration else expiration.plusSeconds(3600),
        )
      }

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-1")

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `resolveIdentity propagates a supplier exception`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        throw RuntimeException("credential chain failed")
      }

    val exception = assertFailsWith<ExecutionException> { provider.resolveIdentity().get() }

    assertThat(exception).hasCauseThat().hasMessageThat().contains("credential chain failed")
  }

  @Test
  fun `resolveIdentity propagates a failed refresh`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        failedRefresh("credential chain failed")
      }

    val exception = assertFailsWith<ExecutionException> { provider.resolveIdentity().get() }

    assertThat(exception).hasCauseThat().hasMessageThat().contains("credential chain failed")
  }

  @Test
  fun `resolveIdentity retries after a supplier exception`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        if (callCount == 1) throw RuntimeException("transient failure")
        completedRefresh("key-$callCount", Instant.parse("2026-06-03T13:00:00Z"))
      }

    assertFailsWith<ExecutionException> { provider.resolveIdentity().get() }
    assertThat(callCount).isEqualTo(1)

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `resolveIdentity retries after a failed refresh`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        if (callCount == 1) failedRefresh("transient failure")
        else completedRefresh("key-$callCount", Instant.parse("2026-06-03T13:00:00Z"))
      }

    assertFailsWith<ExecutionException> { provider.resolveIdentity().get() }
    assertThat(callCount).isEqualTo(1)

    assertThat(provider.resolveIdentity().get().accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `resolveIdentity only refreshes when expired given a zero refresh margin`() {
    val expiration = Instant.parse("2026-06-03T13:00:00Z")
    val justBeforeExpiry = expiration.minusSeconds(1)
    val clock = Clock.fixed(justBeforeExpiry, ZoneOffset.UTC)
    var callCount = 0
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ZERO, clock = clock) {
        callCount++
        completedRefresh("key-$callCount", expiration)
      }

    val first = provider.resolveIdentity().get()
    val second = provider.resolveIdentity().get()

    assertThat(second).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `resolveIdentity shares a refresh that is still in flight`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    var callCount = 0
    val refresh = CompletableFuture<TimeBoundCredentials>()
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount++
        refresh
      }

    val first = provider.resolveIdentity()
    val second = provider.resolveIdentity()
    refresh.complete(timeBoundCredentials("key-1", Instant.parse("2026-06-03T13:00:00Z")))

    assertThat(first.get().accessKeyId()).isEqualTo("key-1")
    assertThat(second.get()).isSameInstanceAs(first.get())
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `resolveIdentity starts a single refresh for concurrent callers`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val callCount = AtomicInteger()
    val refresh = CompletableFuture<TimeBoundCredentials>()
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        callCount.incrementAndGet()
        refresh
      }
    // Every caller waits for the others to arrive before resolving, so they contend rather than
    // running one after another. No caller can complete the refresh, so none of them can observe
    // cached credentials instead of contending.
    val arrived = CountDownLatch(THREAD_COUNT)
    val executor: ExecutorService = Executors.newFixedThreadPool(THREAD_COUNT)

    try {
      val resolutions: List<Future<AwsCredentialsIdentity>> =
        (1..THREAD_COUNT).map {
          executor.submit(
            Callable {
              arrived.countDown()
              arrived.await()
              provider.resolveIdentity().get()
            }
          )
        }
      refresh.complete(timeBoundCredentials("key-1", Instant.parse("2026-06-03T13:00:00Z")))

      for (resolution in resolutions) {
        assertThat(resolution.get().accessKeyId()).isEqualTo("key-1")
      }
      assertThat(callCount.get()).isEqualTo(1)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  fun `resolveCredentials throws`() {
    val clock = Clock.fixed(Instant.parse("2026-06-03T12:00:00Z"), ZoneOffset.UTC)
    val provider =
      RefreshableAwsCredentialsProvider(refreshMargin = Duration.ofMinutes(5), clock = clock) {
        completedRefresh("key-1", Instant.parse("2026-06-03T13:00:00Z"))
      }

    assertFailsWith<UnsupportedOperationException> { provider.resolveCredentials() }
  }

  private fun timeBoundCredentials(accessKeyId: String, expiration: Instant) =
    TimeBoundCredentials(
      credentials = AwsSessionCredentials.create(accessKeyId, "secret", "token"),
      expiration = expiration,
    )

  private fun completedRefresh(
    accessKeyId: String,
    expiration: Instant,
  ): CompletableFuture<TimeBoundCredentials> =
    CompletableFuture.completedFuture(timeBoundCredentials(accessKeyId, expiration))

  private fun failedRefresh(message: String): CompletableFuture<TimeBoundCredentials> =
    CompletableFuture.failedFuture(RuntimeException(message))

  companion object {
    private const val THREAD_COUNT = 8
  }
}

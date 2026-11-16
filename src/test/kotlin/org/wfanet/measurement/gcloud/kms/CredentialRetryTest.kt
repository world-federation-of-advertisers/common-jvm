// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.kms

import com.google.api.client.http.HttpHeaders
import com.google.api.client.http.HttpResponseException
import com.google.auth.Retryable
import com.google.common.truth.Truth.assertThat
import java.io.IOException
import java.time.Duration
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import reactor.test.scheduler.VirtualTimeScheduler
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.services.sts.model.StsException

@RunWith(JUnit4::class)
class CredentialRetryTest {
  @Test
  fun `retries temporary HTTP responses`() {
    for (status in listOf(408, 429, 500, 502, 503, 504)) {
      assertRecovery(httpFailure(status, null), Duration.ofSeconds(61))
    }
  }

  @Test
  fun `does not retry permanent HTTP responses`() {
    for (status in listOf(400, 401, 403, 404, 501, 505)) {
      assertNoRetry(httpFailure(status, null))
    }
  }

  @Test
  fun `uses the public authentication retry hint`() {
    assertRecovery(AuthFailure(true), Duration.ofSeconds(2))
    assertNoRetry(AuthFailure(false))
  }

  @Test
  fun `does not classify generic I O or nested HTTP failures as retryable`() {
    assertNoRetry(IOException("opaque failure"))
    assertNoRetry(IOException("wrapped failure", httpFailure(503, null)))
    assertNoRetry(IllegalStateException("configuration failure"))
  }

  @Test
  fun `HTTP status takes precedence over an authentication retry hint`() {
    assertNoRetry(RetryableHttpFailure(403, true))
    assertRecovery(RetryableHttpFailure(503, false), Duration.ofSeconds(2))
  }

  @Test
  fun `retries STS service errors and named throttling errors`() {
    assertRecovery(stsFailure(503, "ServiceUnavailable", null), Duration.ofSeconds(2))
    assertRecovery(stsFailure(400, "Throttling", null), Duration.ofSeconds(61))
  }

  @Test
  fun `does not retry permanent STS failures`() {
    assertNoRetry(stsFailure(403, "AccessDenied", null))
    assertNoRetry(stsFailure(400, "ValidationError", null))
    assertNoRetry(stsFailure(501, "NotImplemented", null))
  }

  @Test
  fun `quota failures wait at least one minute`() {
    for (failure in listOf(httpFailure(429, "1"), stsFailure(400, "Throttling", "1"))) {
      val attempts = AtomicInteger()
      StepVerifier.withVirtualTime { retrying(attempts, failure, 1) }
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(59))
        .then { assertThat(attempts.get()).isEqualTo(1) }
        .thenAwait(Duration.ofSeconds(2))
        .expectNext("success")
        .verifyComplete()
      assertThat(attempts.get()).isEqualTo(2)
    }
  }

  @Test
  fun `honors Retry-After seconds from HTTP and STS headers`() {
    for (failure in listOf(httpFailure(503, "75"), stsFailure(503, "ServiceUnavailable", "75"))) {
      val attempts = AtomicInteger()
      StepVerifier.withVirtualTime { retrying(attempts, failure, 1) }
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(74))
        .then { assertThat(attempts.get()).isEqualTo(1) }
        .thenAwait(Duration.ofSeconds(2))
        .expectNext("success")
        .verifyComplete()
      assertThat(attempts.get()).isEqualTo(2)
    }
  }

  @Test
  fun `honors Retry-After HTTP dates`() {
    val date =
      DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(90))
    val attempts = AtomicInteger()
    StepVerifier.withVirtualTime { retrying(attempts, httpFailure(503, date), 1) }
      .expectSubscription()
      .thenAwait(Duration.ofSeconds(80))
      .then { assertThat(attempts.get()).isEqualTo(1) }
      .thenAwait(Duration.ofSeconds(12))
      .expectNext("success")
      .verifyComplete()
    assertThat(attempts.get()).isEqualTo(2)
  }

  @Test
  fun `absent invalid negative and past Retry-After use backoff`() {
    val past =
      DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC).minusDays(1))
    for (value in listOf(null, "invalid", "-1", past)) {
      val attempts = AtomicInteger()
      StepVerifier.withVirtualTime { retrying(attempts, httpFailure(503, value), 1) }
        .expectSubscription()
        .thenAwait(Duration.ofMillis(499))
        .then { assertThat(attempts.get()).isEqualTo(1) }
        .thenAwait(Duration.ofSeconds(2))
        .expectNext("success")
        .verifyComplete()
      assertThat(attempts.get()).isEqualTo(2)
    }
  }

  @Test
  fun `allows three retries and preserves the original failure on exhaustion`() {
    val failure = httpFailure(503, null)
    val attempts = AtomicInteger()
    StepVerifier.withVirtualTime { retrying(attempts, failure, Int.MAX_VALUE) }
      .expectSubscription()
      .thenAwait(Duration.ofSeconds(20))
      .expectErrorMatches { it === failure }
      .verify()
    assertThat(attempts.get()).isEqualTo(4)
  }

  @Test
  fun `long Retry-After respects the enclosing refresh deadline`() {
    for (value in listOf("600", Long.MAX_VALUE.toString())) {
      val attempts = AtomicInteger()
      StepVerifier.withVirtualTime {
          retrying(attempts, httpFailure(503, value), 1).timeout(CredentialRetry.REFRESH_TIMEOUT)
        }
        .expectSubscription()
        .thenAwait(Duration.ofMinutes(2))
        .expectError(java.util.concurrent.TimeoutException::class.java)
        .verify()
      assertThat(attempts.get()).isEqualTo(1)
    }
  }

  @Test
  fun `cancelling during backoff prevents another attempt`() {
    val scheduler = VirtualTimeScheduler.getOrSet()
    try {
      val attempts = AtomicInteger()
      val future = retrying(attempts, httpFailure(429, null), 1).toFuture()
      assertThat(attempts.get()).isEqualTo(1)
      future.cancel(false)
      scheduler.advanceTimeBy(Duration.ofMinutes(3))
      assertThat(attempts.get()).isEqualTo(1)
      assertThat(future.isCancelled).isTrue()
    } finally {
      VirtualTimeScheduler.reset()
    }
  }

  private fun assertRecovery(failure: Throwable, delay: Duration) {
    val attempts = AtomicInteger()
    StepVerifier.withVirtualTime { retrying(attempts, failure, 1) }
      .expectSubscription()
      .thenAwait(delay)
      .expectNext("success")
      .verifyComplete()
    assertThat(attempts.get()).isEqualTo(2)
  }

  private fun assertNoRetry(failure: Throwable) {
    val attempts = AtomicInteger()
    StepVerifier.withVirtualTime { retrying(attempts, failure, 1) }
      .expectSubscription()
      .expectErrorMatches { it === failure }
      .verify()
    assertThat(attempts.get()).isEqualTo(1)
  }

  private fun retrying(attempts: AtomicInteger, failure: Throwable, failures: Int): Mono<String> =
    Mono.defer {
        if (attempts.incrementAndGet() <= failures) Mono.error(failure) else Mono.just("success")
      }
      .retryWhen(CredentialRetry.create())

  private fun httpFailure(status: Int, retryAfter: String?): HttpResponseException =
    HttpResponseException.Builder(status, null, HttpHeaders().setRetryAfter(retryAfter)).build()

  private fun stsFailure(status: Int, code: String, retryAfter: String?): StsException =
    StsException.builder()
      .apply {
        statusCode(status)
        awsErrorDetails(
          AwsErrorDetails.builder()
            .errorCode(code)
            .sdkHttpResponse(
              SdkHttpResponse.builder()
                .statusCode(status)
                .headers(
                  if (retryAfter == null) emptyMap() else mapOf("Retry-After" to listOf(retryAfter))
                )
                .build()
            )
            .build()
        )
      }
      .build() as StsException

  private class AuthFailure(private val retryable: Boolean) :
    IOException("authentication failure"), Retryable {
    override fun isRetryable(): Boolean = retryable

    override fun getRetryCount(): Int = 0
  }

  private class RetryableHttpFailure(status: Int, private val retryable: Boolean) :
    HttpResponseException(Builder(status, null, HttpHeaders())), Retryable {
    override fun isRetryable(): Boolean = retryable

    override fun getRetryCount(): Int = 0
  }
}

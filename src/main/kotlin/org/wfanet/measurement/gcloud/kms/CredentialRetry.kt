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

package org.wfanet.measurement.gcloud.kms

import com.google.api.client.http.HttpResponseException
import com.google.auth.Retryable
import java.net.HttpURLConnection
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.random.Random
import org.wfanet.measurement.common.ExponentialBackoff
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import software.amazon.awssdk.core.retry.RetryPolicyContext
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.services.sts.model.StsException

/** Retry policy shared by the AWS federation credential factories. */
internal object CredentialRetry {
  val REFRESH_TIMEOUT: Duration = Duration.ofMinutes(2)
  private const val MAX_RETRIES = 3L
  private const val HTTP_TOO_MANY_REQUESTS = 429
  private val QUOTA_BACKOFF: Duration = Duration.ofMinutes(1)
  private val AWS_RETRY_CONDITION: RetryCondition = RetryCondition.defaultRetryCondition()
  private val backoff =
    ExponentialBackoff(
      initialDelay = Duration.ofSeconds(1),
      multiplier = 2.0,
      randomnessFactor = 0.5,
      random = Random.Default,
    )

  fun create(): Retry =
    Retry.from { signals ->
      signals.concatMap { signal ->
        val failure = signal.failure()
        val retryable =
          when (failure) {
            is HttpResponseException -> retryableHttpStatus(failure.statusCode)
            is StsException ->
              AWS_RETRY_CONDITION.shouldRetry(
                RetryPolicyContext.builder()
                  .exception(failure)
                  .httpStatusCode(failure.statusCode())
                  .build()
              ) || failure.isRetryableException
            is Retryable -> failure.isRetryable
            else -> false
          }
        if (!retryable || signal.totalRetries() >= MAX_RETRIES) {
          Mono.error<Long>(failure)
        } else {
          val quotaFailure =
            when (failure) {
              is HttpResponseException -> failure.statusCode == HTTP_TOO_MANY_REQUESTS
              is StsException -> failure.isThrottlingException
              else -> false
            }
          val retryAfter =
            when (failure) {
              is HttpResponseException -> failure.headers?.retryAfter
              is StsException ->
                failure
                  .awsErrorDetails()
                  ?.sdkHttpResponse()
                  ?.firstMatchingHeader("Retry-After")
                  ?.orElse(null)
              else -> null
            }
          val delay =
            maxOf(
              backoff.durationForAttempt((signal.totalRetries() + 1).toInt()),
              if (quotaFailure) QUOTA_BACKOFF else Duration.ZERO,
              retryAfterDelay(retryAfter),
            )
          Mono.delay(minOf(delay, REFRESH_TIMEOUT))
        }
      }
    }

  private fun retryableHttpStatus(status: Int): Boolean =
    status == HttpURLConnection.HTTP_CLIENT_TIMEOUT ||
      status == HTTP_TOO_MANY_REQUESTS ||
      AWS_RETRY_CONDITION.shouldRetry(RetryPolicyContext.builder().httpStatusCode(status).build())

  private fun retryAfterDelay(value: String?): Duration {
    if (value == null) return Duration.ZERO
    val seconds = value.toLongOrNull()
    if (seconds != null) return Duration.ofSeconds(seconds.coerceAtLeast(0))
    return try {
      maxOf(
        Duration.ZERO,
        Duration.between(
          Instant.now(),
          Instant.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(value)),
        ),
      )
    } catch (e: DateTimeParseException) {
      Duration.ZERO
    }
  }
}

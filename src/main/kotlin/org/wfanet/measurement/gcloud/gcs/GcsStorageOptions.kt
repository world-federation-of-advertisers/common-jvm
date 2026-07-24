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

package org.wfanet.measurement.gcloud.gcs

import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.StorageOptions
import io.opentelemetry.api.OpenTelemetry
import java.time.Duration

/**
 * Resilience configuration -- retry budget and per-attempt timeouts -- applied to every
 * [StorageOptions] built by [buildStorageOptions]. Defaults are generous so legitimate slow reads
 * are not broken, and are overridable.
 */
data class GcsStorageRetryConfig(
  /**
   * Timeout to establish a connection. Maps to [HttpTransportOptions.Builder.setConnectTimeout].
   */
  val connectTimeout: Duration = Duration.ofSeconds(15),
  /**
   * HTTP socket idle-read bound (max time with no bytes); inert on the gRPC transport. Maps to
   * [HttpTransportOptions.Builder.setReadTimeout].
   */
  val readTimeout: Duration = Duration.ofSeconds(30),
  /**
   * gRPC per-attempt (whole-attempt) deadline; inert on the HTTP transport. Defaults to the
   * gax/storage `readObject` default of 60s. Maps to [RetrySettings.Builder.setInitialRpcTimeout] /
   * [RetrySettings.Builder.setMaxRpcTimeout].
   */
  val rpcTimeout: Duration = Duration.ofSeconds(60),
  /** Total retry budget across all attempts. Maps to [RetrySettings.Builder.setTotalTimeout]. */
  val totalTimeout: Duration = Duration.ofSeconds(180),
  /** Maximum number of attempts. Maps to [RetrySettings.Builder.setMaxAttempts]. */
  val maxAttempts: Int = 6,
  /** Delay before the first retry. Maps to [RetrySettings.Builder.setInitialRetryDelay]. */
  val initialRetryDelay: Duration = Duration.ofSeconds(1),
  /** Maximum backoff delay between retries. Maps to [RetrySettings.Builder.setMaxRetryDelay]. */
  val maxRetryDelay: Duration = Duration.ofSeconds(32),
  /** Backoff multiplier. Maps to [RetrySettings.Builder.setRetryDelayMultiplier]. */
  val retryDelayMultiplier: Double = 2.0,
) {
  init {
    require(!connectTimeout.isNegative && !connectTimeout.isZero) {
      "connectTimeout must be positive, got $connectTimeout"
    }
    require(!readTimeout.isNegative && !readTimeout.isZero) {
      "readTimeout must be positive, got $readTimeout"
    }
    require(!rpcTimeout.isNegative && !rpcTimeout.isZero) {
      "rpcTimeout must be positive, got $rpcTimeout"
    }
    require(!initialRetryDelay.isNegative && !initialRetryDelay.isZero) {
      "initialRetryDelay must be positive, got $initialRetryDelay"
    }
    require(maxRetryDelay >= initialRetryDelay) {
      "maxRetryDelay ($maxRetryDelay) must be >= initialRetryDelay ($initialRetryDelay)"
    }
    require(maxAttempts >= 1) { "maxAttempts must be at least 1, got $maxAttempts" }
    require(retryDelayMultiplier >= 1.0) {
      "retryDelayMultiplier must be >= 1.0, got $retryDelayMultiplier"
    }
    require(totalTimeout >= connectTimeout.plus(readTimeout)) {
      "totalTimeout ($totalTimeout) must be >= connectTimeout + readTimeout " +
        "(${connectTimeout.plus(readTimeout)}) so at least one full HTTP attempt fits in the budget"
    }
    require(totalTimeout >= rpcTimeout) {
      "totalTimeout ($totalTimeout) must be >= rpcTimeout ($rpcTimeout) so at least one full gRPC " +
        "attempt fits in the budget"
    }
  }

  /** [RetrySettings] derived from this config. */
  fun toRetrySettings(): RetrySettings =
    RetrySettings.newBuilder()
      .setInitialRetryDelayDuration(initialRetryDelay)
      .setRetryDelayMultiplier(retryDelayMultiplier)
      .setMaxRetryDelayDuration(maxRetryDelay)
      .setMaxAttempts(maxAttempts)
      .setTotalTimeoutDuration(totalTimeout)
      // readTimeout is the HTTP socket idle-read bound (applied via HttpTransportOptions); the RPC
      // timeout is the gRPC per-attempt whole-attempt deadline, so it maps to rpcTimeout, not
      // readTimeout. The RPC timeout is inert on the HTTP/Apiary transport.
      .setInitialRpcTimeoutDuration(rpcTimeout)
      .setMaxRpcTimeoutDuration(rpcTimeout)
      .setRpcTimeoutMultiplier(1.0)
      .build()

  /** [HttpTransportOptions] derived from this config (HTTP/Apiary transport only). */
  fun toHttpTransportOptions(): HttpTransportOptions =
    HttpTransportOptions.newBuilder()
      .setConnectTimeout(connectTimeout.toMillis().toInt())
      .setReadTimeout(readTimeout.toMillis().toInt())
      .build()

  /**
   * Builds [StorageOptions] for Google Cloud Storage with this config's retry/timeout settings.
   *
   * @param projectId GCS project id, or `null` to let the library resolve it from the environment.
   * @param useGrpc whether to use the gRPC transport instead of HTTP.
   * @param openTelemetry [OpenTelemetry] instance for client metrics/traces, or `null`.
   */
  fun buildStorageOptions(
    projectId: String? = null,
    useGrpc: Boolean = false,
    openTelemetry: OpenTelemetry? = null,
  ): StorageOptions {
    val builder: StorageOptions.Builder =
      if (useGrpc) {
        StorageOptions.grpc().setEnableGrpcClientMetrics(true)
      } else {
        StorageOptions.http().setTransportOptions(toHttpTransportOptions())
      }
    builder.setRetrySettings(toRetrySettings())
    if (projectId != null) {
      builder.setProjectId(projectId)
    }
    if (openTelemetry != null) {
      builder.setOpenTelemetry(openTelemetry)
    }
    return builder.build()
  }

  companion object {
    /** The resilient default configuration used for all callers unless overridden. */
    val DEFAULT = GcsStorageRetryConfig()
  }
}

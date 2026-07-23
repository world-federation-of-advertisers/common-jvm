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
 * Resilience configuration applied to every [StorageOptions] built by [buildGcsStorageOptions].
 *
 * ## Why this exists
 * The `google-cloud-storage` library defaults leave a Cloud Storage read vulnerable to transient
 * connection glitches (observed in production as `Broken pipe` and `Remote host terminated the
 * handshake` thrown from `BaseStorageReadChannel.read`):
 * * The default retry budget is `totalTimeout = 50s` / `maxAttempts = 6`
 *   (`com.google.cloud.ServiceOptions.getDefaultRetrySettingsBuilder`).
 * * The default HTTP transport
 *   ([com.google.cloud.storage.HttpStorageOptions.HttpStorageDefaults.getDefaultTransportOptions])
 *   sets no explicit socket read timeout, so each socket read falls back to the google-http-client
 *   `HttpRequest` default. That default bounds the idle gap between successive reads, not the total
 *   download time, so a slow-but-progressing read can consume the entire 50s retry budget before a
 *   second attempt can start. The read then fails after a single attempt even though the library's
 *   read channel would otherwise resume mid-stream at the current offset via a `Range` request
 *   ([com.google.cloud.storage.ApiaryUnbufferedReadableByteChannel], which tracks `position` and
 *   re-opens with `withNewBeginOffset`).
 *
 * ## What bounds an attempt (HTTP vs gRPC)
 * The two transports bound a single attempt differently:
 * * On the HTTP/Apiary transport, [readTimeout] is the transport socket read timeout
 *   ([HttpTransportOptions.Builder.setReadTimeout] -> `HttpRequest.setReadTimeout` ->
 *   `HttpURLConnection` `SO_TIMEOUT`), applied via [StorageOptions.Builder.setTransportOptions]. It
 *   is an idle-gap bound: it aborts a read that stalls with no bytes for that long. The
 *   [RetrySettings] RPC timeouts are inert here (they are not enforced on the blocking Apiary
 *   read).
 * * On the gRPC transport, there is no socket read timeout; the per-attempt bound is the
 *   [RetrySettings] RPC timeout ([rpcTimeout]), which is a whole-attempt wall-clock deadline (not
 *   an idle gap). The gRPC read channel also resumes at the current offset across attempts
 *   ([com.google.cloud.storage.GapicUnbufferedReadableByteChannel] re-opens with `read_offset`), so
 *   a large read completes in chunks within [totalTimeout].
 *
 * In both cases [RetrySettings] governs the retry budget (`totalTimeout`, `maxAttempts`) and the
 * jittered exponential backoff between attempts.
 *
 * ## Scope
 * These [RetrySettings] and transport timeouts apply to ALL Storage operations (reads, writes, and
 * metadata); the mid-stream resume-at-offset described above is the read-specific part, and the
 * library's default retry strategy only retries idempotent writes, so widening the retry budget
 * introduces no new double-write risk.
 *
 * ## Defaults
 * The defaults are deliberately generous so that legitimate slow reads are not broken, while still
 * bounding each attempt so a stalled socket is aborted promptly and several bounded attempts fit in
 * the budget:
 * * [readTimeout] = 30s (HTTP idle-read bound): a healthy Cloud Storage stream delivers data
 *   continuously, so 30s of silence indicates a real stall.
 * * [connectTimeout] = 15s.
 * * [rpcTimeout] = 60s (gRPC per-attempt whole-attempt deadline): the gax/storage `readObject`
 *   default; generous enough for a chunk of a large read, which resumes at offset on the next
 *   attempt.
 * * [totalTimeout] = 180s: room for several bounded attempts (each resuming at the current offset).
 * * [maxAttempts] = 6 with exponential backoff ([initialRetryDelay] 1s, [maxRetryDelay] 32s,
 *   [retryDelayMultiplier] 2.0). gax applies jitter to this backoff by default.
 *
 * All values are overridable so a caller (e.g. the Results-Fulfiller) can tune tighter later.
 */
data class GcsStorageRetryConfig(
  /**
   * Timeout to establish a connection. Maps to [HttpTransportOptions.Builder.setConnectTimeout].
   */
  val connectTimeout: Duration = Duration.ofSeconds(15),
  /**
   * HTTP/Apiary per-attempt socket read timeout (an idle-gap bound). Maps to
   * [HttpTransportOptions.Builder.setReadTimeout]; this is the knob that aborts a stalled socket
   * read on the HTTP transport. Inert on the gRPC transport (see [rpcTimeout]).
   */
  val readTimeout: Duration = Duration.ofSeconds(30),
  /**
   * gRPC per-attempt RPC timeout (a whole-attempt wall-clock deadline, not an idle gap). Maps to
   * [RetrySettings.Builder.setInitialRpcTimeout]/[RetrySettings.Builder.setMaxRpcTimeout]. Defaults
   * to 60s, the gax/storage `readObject` default. Inert on the HTTP transport (see [readTimeout]).
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

  companion object {
    /** The resilient default configuration used for all callers unless overridden. */
    val DEFAULT = GcsStorageRetryConfig()
  }
}

/**
 * Builds [StorageOptions] for Google Cloud Storage with resilient retry/timeout settings applied by
 * default (see [GcsStorageRetryConfig]).
 *
 * @param projectId GCS project id, or `null` to let the library resolve it from the environment.
 * @param useGrpc whether to use the gRPC transport instead of HTTP. The HTTP transport read/connect
 *   timeouts only apply to the HTTP transport; on gRPC the per-attempt bound is the RPC timeout.
 * @param openTelemetry [OpenTelemetry] instance for client metrics/traces, or `null`.
 * @param retryConfig resilience configuration; defaults to [GcsStorageRetryConfig.DEFAULT].
 */
fun buildGcsStorageOptions(
  projectId: String? = null,
  useGrpc: Boolean = false,
  openTelemetry: OpenTelemetry? = null,
  retryConfig: GcsStorageRetryConfig = GcsStorageRetryConfig.DEFAULT,
): StorageOptions {
  val builder: StorageOptions.Builder =
    if (useGrpc) {
      StorageOptions.grpc().setEnableGrpcClientMetrics(true)
    } else {
      StorageOptions.http().setTransportOptions(retryConfig.toHttpTransportOptions())
    }
  builder.setRetrySettings(retryConfig.toRetrySettings())
  if (projectId != null) {
    builder.setProjectId(projectId)
  }
  if (openTelemetry != null) {
    builder.setOpenTelemetry(openTelemetry)
  }
  return builder.build()
}

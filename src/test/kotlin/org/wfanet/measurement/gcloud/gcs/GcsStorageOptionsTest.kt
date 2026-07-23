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

import com.google.cloud.NoCredentials
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import java.io.IOException
import java.io.InputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten

/**
 * Tests for [GcsStorageRetryConfig] / [buildGcsStorageOptions].
 *
 * The read-path tests reproduce the production glitch (a Cloud Storage read that fails on a
 * transient connection reset) against an in-process HTTP server ([FakeGcsServer]) that speaks just
 * enough of the JSON API for [GcsStorageClient] to fetch and read a blob. The client is pointed at
 * the fake via [StorageOptions.Builder.setHost] (mirroring
 * `org.wfanet.measurement.gcloud.gcs.testing.StorageEmulator`).
 */
@RunWith(JUnit4::class)
class GcsStorageOptionsTest {
  @Test
  fun `default config has resilient values`() {
    val config = GcsStorageRetryConfig.DEFAULT
    assertThat(config.connectTimeout).isEqualTo(Duration.ofSeconds(15))
    assertThat(config.readTimeout).isEqualTo(Duration.ofSeconds(30))
    assertThat(config.rpcTimeout).isEqualTo(Duration.ofSeconds(60))
    assertThat(config.totalTimeout).isEqualTo(Duration.ofSeconds(180))
    assertThat(config.maxAttempts).isEqualTo(6)
    assertThat(config.initialRetryDelay).isEqualTo(Duration.ofSeconds(1))
    assertThat(config.maxRetryDelay).isEqualTo(Duration.ofSeconds(32))
    assertThat(config.retryDelayMultiplier).isEqualTo(2.0)
  }

  @Test
  fun `toRetrySettings maps config values`() {
    val retrySettings = GcsStorageRetryConfig.DEFAULT.toRetrySettings()
    assertThat(retrySettings.totalTimeoutDuration).isEqualTo(Duration.ofSeconds(180))
    assertThat(retrySettings.maxAttempts).isEqualTo(6)
    assertThat(retrySettings.initialRetryDelayDuration).isEqualTo(Duration.ofSeconds(1))
    assertThat(retrySettings.maxRetryDelayDuration).isEqualTo(Duration.ofSeconds(32))
    assertThat(retrySettings.retryDelayMultiplier).isEqualTo(2.0)
    // The RPC timeouts (the gRPC per-attempt deadline) come from rpcTimeout (60s), NOT from the
    // HTTP readTimeout (30s).
    assertThat(retrySettings.initialRpcTimeoutDuration).isEqualTo(Duration.ofSeconds(60))
    assertThat(retrySettings.maxRpcTimeoutDuration).isEqualTo(Duration.ofSeconds(60))
  }

  @Test
  fun `toHttpTransportOptions maps timeouts to millis`() {
    val transportOptions = GcsStorageRetryConfig.DEFAULT.toHttpTransportOptions()
    assertThat(transportOptions.connectTimeout).isEqualTo(15_000)
    assertThat(transportOptions.readTimeout).isEqualTo(30_000)
  }

  @Test
  fun `buildGcsStorageOptions applies resilient settings by default`() {
    val options = buildGcsStorageOptions(projectId = "some-project")

    assertThat(options.retrySettings.totalTimeoutDuration).isEqualTo(Duration.ofSeconds(180))
    assertThat(options.retrySettings.maxAttempts).isEqualTo(6)
    val transportOptions = options.transportOptions as HttpTransportOptions
    assertThat(transportOptions.readTimeout).isEqualTo(30_000)
    assertThat(transportOptions.connectTimeout).isEqualTo(15_000)
  }

  @Test
  fun `buildGcsStorageOptions honors an overridden config`() {
    val config =
      GcsStorageRetryConfig.DEFAULT.copy(readTimeout = Duration.ofSeconds(5), maxAttempts = 3)

    val options = buildGcsStorageOptions(projectId = "some-project", retryConfig = config)

    assertThat(options.retrySettings.maxAttempts).isEqualTo(3)
    assertThat((options.transportOptions as HttpTransportOptions).readTimeout).isEqualTo(5_000)
  }

  @Test
  fun `config rejects a non-positive read timeout`() {
    assertThrows(IllegalArgumentException::class.java) {
      GcsStorageRetryConfig(readTimeout = Duration.ZERO)
    }
  }

  @Test
  fun `config rejects a non-positive rpc timeout`() {
    assertThrows(IllegalArgumentException::class.java) {
      GcsStorageRetryConfig(rpcTimeout = Duration.ZERO)
    }
  }

  @Test
  fun `config rejects a total timeout smaller than the rpc timeout`() {
    // connect + read (15s + 30s = 45s) fits within total (50s), but the 60s default rpcTimeout does
    // not, so a full gRPC attempt would not fit and the config must be rejected.
    assertThrows(IllegalArgumentException::class.java) {
      GcsStorageRetryConfig(totalTimeout = Duration.ofSeconds(50))
    }
  }

  @Test
  fun `config rejects a total timeout smaller than connect plus read timeout`() {
    // total (40s) exceeds the read timeout (30s) but is still below connect + read (15s + 30s), so
    // it does not leave room for one full attempt and must be rejected.
    assertThrows(IllegalArgumentException::class.java) {
      GcsStorageRetryConfig(
        connectTimeout = Duration.ofSeconds(15),
        readTimeout = Duration.ofSeconds(30),
        totalTimeout = Duration.ofSeconds(40),
      )
    }
  }

  @Test
  fun `default config satisfies the connect plus read timeout budget`() {
    // Constructing the default config must not throw: 180s >= 15s + 30s.
    val config = GcsStorageRetryConfig()
    assertThat(config.totalTimeout).isAtLeast(config.connectTimeout.plus(config.readTimeout))
  }

  @Test
  fun `read resumes at offset after a mid-stream connection drop`() = runBlocking {
    val content = Random(1).nextBytes(64 * 1024)
    FakeGcsServer(content).use { server ->
      // On the first media request, deliver a prefix then reset the connection (RST). The client
      // must resume the download at the current offset via a Range request.
      server.dropFirstAttemptAfterBytes = 16 * 1024

      val client = gcsStorageClient(server.host, TEST_CONFIG)
      val blob = checkNotNull(client.getBlob(OBJECT_NAME)) { "Blob not found" }

      val read = blob.read().flatten().toByteArray()

      // Full blob read back with correct bytes and no double-count.
      assertThat(read).isEqualTo(content)
      // The retried media request carried a Range starting exactly at the bytes delivered before
      // the drop, proving resume-at-offset.
      assertThat(server.observedMediaRangeStarts)
        .containsExactly(0L, (16 * 1024).toLong())
        .inOrder()
    }
  }

  @Test
  fun `read recovers from a stalled attempt via the per-attempt read timeout`() = runBlocking {
    val content = Random(2).nextBytes(32 * 1024)
    FakeGcsServer(content).use { server ->
      // The first media request sends response headers then no body and holds the connection open.
      // With a short per-attempt read timeout the client aborts that attempt and retries.
      server.stallFirstAttemptMillis = 10_000

      // Short read timeout keeps the test fast; a large read timeout with no retry would hang ~10s.
      val config = TEST_CONFIG.copy(readTimeout = Duration.ofSeconds(2))
      val client = gcsStorageClient(server.host, config)
      val blob = checkNotNull(client.getBlob(OBJECT_NAME)) { "Blob not found" }

      val read = blob.read().flatten().toByteArray()

      assertThat(read).isEqualTo(content)
      // More than one media request proves the stalled first attempt was aborted by the read
      // timeout and a retry fired (rather than blocking on the single stalled attempt).
      assertThat(server.mediaRequestCount).isAtLeast(2)
    }
  }

  private fun gcsStorageClient(host: String, config: GcsStorageRetryConfig): GcsStorageClient {
    val storageOptions =
      StorageOptions.http()
        .setHost(host)
        .setProjectId("fake-project")
        .setCredentials(NoCredentials.getInstance())
        .setTransportOptions(config.toHttpTransportOptions())
        .setRetrySettings(config.toRetrySettings())
        .build()
    return GcsStorageClient(storageOptions.service, BUCKET)
  }

  /**
   * Minimal in-process Cloud Storage HTTP endpoint backed by a raw [ServerSocket], used to inject
   * controllable connection failures into the read path.
   *
   * Each request is served on its own thread. A request whose query contains `alt=media` is treated
   * as a media download; anything else is treated as an object-metadata fetch.
   */
  private class FakeGcsServer(private val objectBytes: ByteArray) : AutoCloseable {
    private val serverSocket = ServerSocket(0)
    private val mediaAttempts = AtomicInteger(0)
    private val rangeStarts = CopyOnWriteArrayList<Long>()

    /** If `>= 0`, the first media attempt sends this many bytes then resets the connection. */
    @Volatile var dropFirstAttemptAfterBytes: Int = -1

    /** If `> 0`, the first media attempt sends headers then stalls for this many millis. */
    @Volatile var stallFirstAttemptMillis: Long = 0

    val host: String
      get() = "http://localhost:${serverSocket.localPort}"

    /** Range start offset observed for each media request, in arrival order. */
    val observedMediaRangeStarts: List<Long>
      get() = rangeStarts.toList()

    val mediaRequestCount: Int
      get() = mediaAttempts.get()

    init {
      thread(isDaemon = true, name = "fake-gcs-accept") { acceptLoop() }
    }

    private fun acceptLoop() {
      while (!serverSocket.isClosed) {
        val socket =
          try {
            serverSocket.accept()
          } catch (e: IOException) {
            return // Socket closed.
          }
        thread(isDaemon = true, name = "fake-gcs-conn") { handle(socket) }
      }
    }

    private fun handle(socket: Socket) {
      try {
        socket.tcpNoDelay = true
        val requestHead = readRequestHead(socket.getInputStream()) ?: return
        val requestLine = requestHead.substringBefore("\r\n")
        if (requestLine.contains("alt=media")) {
          val rangeStart = parseRangeStart(requestHead)
          rangeStarts.add(rangeStart)
          val attempt = mediaAttempts.incrementAndGet()
          when {
            attempt == 1 && stallFirstAttemptMillis > 0 ->
              serveHeadersThenStall(socket, rangeStart, stallFirstAttemptMillis)
            attempt == 1 && dropFirstAttemptAfterBytes >= 0 ->
              serveDropThenReset(socket, rangeStart, dropFirstAttemptAfterBytes)
            else -> serveMedia(socket, rangeStart)
          }
        } else {
          serveMetadata(socket)
        }
      } catch (e: IOException) {
        // Client went away; nothing to do.
      } finally {
        try {
          socket.close()
        } catch (e: IOException) {
          // Ignore.
        }
      }
    }

    private fun serveMetadata(socket: Socket) {
      val json =
        """{"kind":"storage#object","bucket":"$BUCKET","name":"$OBJECT_NAME","generation":"1",""" +
          """"metageneration":"1","contentType":"application/octet-stream",""" +
          """"size":"${objectBytes.size}","timeCreated":"2024-01-01T00:00:00.000Z",""" +
          """"updated":"2024-01-01T00:00:00.000Z"}"""
      val body = json.toByteArray(StandardCharsets.UTF_8)
      val out = socket.getOutputStream()
      out.write(
        ("HTTP/1.1 200 OK\r\n" +
            "Content-Type: application/json; charset=UTF-8\r\n" +
            "Content-Length: ${body.size}\r\n" +
            "Connection: close\r\n\r\n")
          .toByteArray(StandardCharsets.UTF_8)
      )
      out.write(body)
      out.flush()
    }

    private fun serveMedia(socket: Socket, rangeStart: Long) {
      val start = rangeStart.toInt().coerceIn(0, objectBytes.size)
      val slice = objectBytes.copyOfRange(start, objectBytes.size)
      val out = socket.getOutputStream()
      val statusLine = if (start > 0) "HTTP/1.1 206 Partial Content" else "HTTP/1.1 200 OK"
      val headers = StringBuilder()
      headers.append("$statusLine\r\n")
      headers.append("Content-Type: application/octet-stream\r\n")
      headers.append("Content-Length: ${slice.size}\r\n")
      if (start > 0) {
        headers.append(
          "Content-Range: bytes $start-${objectBytes.size - 1}/${objectBytes.size}\r\n"
        )
      }
      headers.append("Connection: close\r\n\r\n")
      out.write(headers.toString().toByteArray(StandardCharsets.UTF_8))
      out.write(slice)
      out.flush()
    }

    private fun serveDropThenReset(socket: Socket, rangeStart: Long, partialLen: Int) {
      val start = rangeStart.toInt().coerceIn(0, objectBytes.size)
      val remaining = objectBytes.size - start
      val out = socket.getOutputStream()
      // Declare the full remaining length but only send a prefix, then abort with a TCP RST so the
      // client observes a mid-stream connection reset (a retryable SocketException).
      out.write(
        ("HTTP/1.1 200 OK\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "Content-Length: $remaining\r\n" +
            "Connection: close\r\n\r\n")
          .toByteArray(StandardCharsets.UTF_8)
      )
      out.write(objectBytes, start, partialLen.coerceAtMost(remaining))
      out.flush()
      socket.setSoLinger(true, 0)
      socket.close()
    }

    private fun serveHeadersThenStall(socket: Socket, rangeStart: Long, stallMillis: Long) {
      val start = rangeStart.toInt().coerceIn(0, objectBytes.size)
      val remaining = objectBytes.size - start
      val out = socket.getOutputStream()
      out.write(
        ("HTTP/1.1 200 OK\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "Content-Length: $remaining\r\n" +
            "Connection: close\r\n\r\n")
          .toByteArray(StandardCharsets.UTF_8)
      )
      out.flush()
      // Send no body and hold the connection open so the client's socket read stalls.
      Thread.sleep(stallMillis)
    }

    private fun readRequestHead(input: InputStream): String? {
      val builder = StringBuilder()
      while (true) {
        val c = input.read()
        if (c == -1) {
          return builder.ifEmpty { null }?.toString()
        }
        builder.append(c.toChar())
        if (builder.endsWith("\r\n\r\n")) {
          return builder.toString()
        }
      }
    }

    private fun parseRangeStart(requestHead: String): Long {
      val rangeLine =
        requestHead.lineSequence().firstOrNull { it.startsWith("Range:", ignoreCase = true) }
          ?: return 0L
      val spec = rangeLine.substringAfter("bytes=", "").trim()
      return spec.substringBefore("-", "").trim().toLongOrNull() ?: 0L
    }

    override fun close() {
      serverSocket.close()
    }
  }

  companion object {
    private const val BUCKET = "test-bucket"
    private const val OBJECT_NAME = "test-object"

    /** Fast but resilient config for read-path tests. */
    private val TEST_CONFIG =
      GcsStorageRetryConfig(
        connectTimeout = Duration.ofSeconds(5),
        readTimeout = Duration.ofSeconds(10),
        totalTimeout = Duration.ofSeconds(60),
        maxAttempts = 5,
        initialRetryDelay = Duration.ofMillis(100),
        maxRetryDelay = Duration.ofSeconds(1),
        retryDelayMultiplier = 2.0,
      )
  }
}

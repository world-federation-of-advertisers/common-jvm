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

package org.wfanet.measurement.common.net

import com.google.common.truth.Truth.assertThat
import java.net.SocketTimeoutException
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Tests for [UnixDomainSocket] and [UnixDomainSocketFactory]. */
@RunWith(JUnit4::class)
class UnixDomainSocketTest {
  private var serverChannel: ServerSocketChannel? = null
  private var serverThread: Thread? = null
  private lateinit var socketPath: Path
  private val idle = CountDownLatch(1)

  /**
   * Binds a socket whose accepted connection is handled by [handler].
   *
   * Bound under /tmp rather than the test tmpdir because a Unix socket path is limited to about 108
   * bytes, which Bazel's much longer tmpdir exceeds. That makes this test non-hermetic: it writes
   * outside the sandbox and would need a short writable path under a remote executor.
   */
  private fun startServer(handler: (SocketChannel) -> Unit) {
    socketPath = Paths.get("/tmp", "uds-test-${System.nanoTime()}.sock")
    val channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    channel.bind(UnixDomainSocketAddress.of(socketPath))
    serverChannel = channel
    serverThread =
      thread(start = true, isDaemon = true) {
        try {
          channel.accept().use(handler)
        } catch (e: Exception) {
          // Expected once the channel is closed in tearDown.
        }
      }
  }

  /** Binds a socket that accepts a connection and then never sends anything. */
  private fun startSilentServer() = startServer { idle.await() }

  @After
  fun tearDown() {
    idle.countDown()
    serverChannel?.close()
    serverThread?.join(TimeUnit.SECONDS.toMillis(5))
    if (this::socketPath.isInitialized) {
      Files.deleteIfExists(socketPath)
    }
  }

  @Test
  fun `exchanges bytes with the server`() {
    startServer { connection ->
      val buffer = ByteBuffer.allocate(64)
      connection.read(buffer)
      buffer.flip()
      val received = StandardCharsets.UTF_8.decode(buffer).toString()
      val reply = ByteBuffer.wrap("echo:$received".toByteArray(StandardCharsets.UTF_8))
      while (reply.hasRemaining()) {
        connection.write(reply)
      }
    }

    UnixDomainSocket(socketPath).use { socket ->
      socket.connect(null)
      socket.getOutputStream().write("ping".toByteArray(StandardCharsets.UTF_8))
      val response = ByteArray(9)
      val count = socket.getInputStream().read(response)

      assertThat(String(response, 0, count, StandardCharsets.UTF_8)).isEqualTo("echo:ping")
    }
  }

  @Test
  fun `read fails once the timeout elapses`() {
    startSilentServer()

    UnixDomainSocket(socketPath).use { socket ->
      socket.connect(null)
      socket.soTimeout = 50

      assertFailsWith<SocketTimeoutException> { socket.getInputStream().read(ByteArray(8)) }
    }
  }

  @Test
  fun `setSoTimeout rejects a negative timeout`() {
    startSilentServer()

    UnixDomainSocket(socketPath).use { socket ->
      assertFailsWith<IllegalArgumentException> { socket.soTimeout = -1 }
    }
  }

  @Test
  fun `getSoTimeout returns what was set`() {
    startSilentServer()

    UnixDomainSocket(socketPath).use { socket ->
      socket.soTimeout = 1234

      assertThat(socket.soTimeout).isEqualTo(1234)
    }
  }

  @Test
  fun `close is idempotent`() {
    startSilentServer()
    val socket = UnixDomainSocket(socketPath)
    socket.connect(null)

    socket.close()
    socket.close()

    assertThat(socket.isClosed).isTrue()
  }

  @Test
  fun `remote address is absent until connected`() {
    startSilentServer()

    UnixDomainSocket(socketPath).use { socket ->
      assertThat(socket.remoteSocketAddress).isNull()
      assertThat(socket.isConnected).isFalse()

      socket.connect(null)

      assertThat(socket.remoteSocketAddress).isEqualTo(UnixDomainSocketAddress.of(socketPath))
      assertThat(socket.isConnected).isTrue()
    }
  }

  @Test
  fun `binding is not supported`() {
    startSilentServer()

    UnixDomainSocket(socketPath).use { socket ->
      assertFailsWith<UnsupportedOperationException> {
        socket.bind(UnixDomainSocketAddress.of(socketPath))
      }
    }
  }

  @Test
  fun `factory produces a socket for the configured path`() {
    startSilentServer()

    UnixDomainSocketFactory(socketPath).createSocket().use { socket ->
      socket.connect(null)

      assertThat(socket).isInstanceOf(UnixDomainSocket::class.java)
      assertThat(socket.remoteSocketAddress).isEqualTo(UnixDomainSocketAddress.of(socketPath))
    }
  }

  @Test
  fun `factory rejects a host and port`() {
    startSilentServer()
    val factory = UnixDomainSocketFactory(socketPath)

    assertFailsWith<UnsupportedOperationException> { factory.createSocket("localhost", 8080) }
  }
}

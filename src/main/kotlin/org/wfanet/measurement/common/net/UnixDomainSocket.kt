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

import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.Socket
import java.net.SocketAddress
import java.net.SocketImpl
import java.net.SocketTimeoutException
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import java.nio.file.Path

/**
 * A [Socket] backed by a Unix domain socket.
 *
 * The JDK exposes Unix domain sockets only as a [SocketChannel]; there is no [Socket] view of one,
 * so HTTP clients that accept a `javax.net.SocketFactory` cannot use them directly. This adapts the
 * channel to the [Socket] surface those clients rely on.
 *
 * The instance is constructed without a [SocketImpl], so every member the caller may touch is
 * overridden here rather than delegated to one.
 *
 * @param socketPath Filesystem path of the Unix domain socket to connect to.
 */
class UnixDomainSocket(private val socketPath: Path) : Socket(null as SocketImpl?) {
  private val channel: SocketChannel = SocketChannel.open(StandardProtocolFamily.UNIX)

  /**
   * Read and write readiness are awaited on separate selectors so that a blocked read cannot
   * consume the readiness notification a concurrent write is waiting for.
   */
  private val readSelector: Selector = Selector.open()
  private val writeSelector: Selector = Selector.open()

  @Volatile private var soTimeoutMillis: Int = 0

  private val socketInputStream: InputStream =
    object : InputStream() {
      private val singleByte = ByteArray(1)

      override fun read(): Int {
        val count = read(singleByte, 0, 1)
        return if (count == -1) -1 else singleByte[0].toInt() and 0xFF
      }

      override fun read(destination: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) {
          return 0
        }
        val buffer = ByteBuffer.wrap(destination, offset, length)
        while (true) {
          val count = channel.read(buffer)
          if (count != 0) {
            return count
          }
          awaitReady(readSelector, "read from")
        }
      }
    }

  private val socketOutputStream: OutputStream =
    object : OutputStream() {
      private val singleByte = ByteArray(1)

      override fun write(value: Int) {
        singleByte[0] = value.toByte()
        write(singleByte, 0, 1)
      }

      override fun write(source: ByteArray, offset: Int, length: Int) {
        val buffer = ByteBuffer.wrap(source, offset, length)
        while (buffer.hasRemaining()) {
          if (channel.write(buffer) == 0) {
            awaitReady(writeSelector, "write to")
          }
        }
      }
    }

  /**
   * Connects to [socketPath], ignoring [endpoint].
   *
   * HTTP clients derive an `InetSocketAddress` from the request URL, which is meaningless for a
   * Unix domain socket; the destination is the path this socket was created with.
   */
  override fun connect(endpoint: SocketAddress?, timeout: Int) {
    channel.connect(UnixDomainSocketAddress.of(socketPath))
    // Non-blocking from here on, so reads and writes can honor SO_TIMEOUT via the selectors.
    channel.configureBlocking(false)
    channel.register(readSelector, SelectionKey.OP_READ)
    channel.register(writeSelector, SelectionKey.OP_WRITE)
  }

  override fun connect(endpoint: SocketAddress?) = connect(endpoint, 0)

  /**
   * Blocks until [selector]'s channel is ready, honoring [soTimeoutMillis].
   *
   * Callers depend on a timeout actually firing: OkHttp probes a pooled connection by setting a 1ms
   * timeout and reading, and would block forever if the timeout were ignored.
   */
  private fun awaitReady(selector: Selector, action: String) {
    selector.selectedKeys().clear()
    val timeout = soTimeoutMillis
    val readyCount = if (timeout > 0) selector.select(timeout.toLong()) else selector.select()
    if (readyCount == 0 && timeout > 0) {
      throw SocketTimeoutException("Timed out waiting to $action $socketPath")
    }
  }

  override fun getInputStream(): InputStream = socketInputStream

  override fun getOutputStream(): OutputStream = socketOutputStream

  override fun setSoTimeout(timeout: Int) {
    require(timeout >= 0) { "Timeout must not be negative" }
    soTimeoutMillis = timeout
  }

  override fun getSoTimeout(): Int = soTimeoutMillis

  override fun isConnected(): Boolean = channel.isConnected

  override fun isClosed(): Boolean = !channel.isOpen

  override fun isInputShutdown(): Boolean = !channel.isOpen

  override fun isOutputShutdown(): Boolean = !channel.isOpen

  override fun getRemoteSocketAddress(): SocketAddress? =
    if (channel.isConnected) UnixDomainSocketAddress.of(socketPath) else null

  override fun getInetAddress(): InetAddress? = null

  override fun close() {
    try {
      channel.close()
    } finally {
      readSelector.close()
      writeSelector.close()
    }
  }
}

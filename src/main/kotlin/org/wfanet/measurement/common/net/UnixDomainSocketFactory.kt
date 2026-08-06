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

import java.net.InetAddress
import java.net.Socket
import java.nio.file.Path
import javax.net.SocketFactory

/**
 * A [SocketFactory] that produces a [UnixDomainSocket] for [socketPath].
 *
 * HTTP clients that accept a [SocketFactory] can be pointed at a Unix domain socket this way; they
 * call the no-argument [createSocket] and then connect it. The destination is fixed at
 * construction, so the overloads taking a host and port cannot be honored and reject the call
 * rather than returning a socket bound somewhere the caller did not ask for.
 *
 * @param socketPath Filesystem path of the Unix domain socket to connect to.
 */
class UnixDomainSocketFactory(private val socketPath: Path) : SocketFactory() {
  override fun createSocket(): Socket = UnixDomainSocket(socketPath)

  override fun createSocket(host: String?, port: Int): Nothing = unsupported()

  override fun createSocket(
    host: String?,
    port: Int,
    localHost: InetAddress?,
    localPort: Int,
  ): Nothing = unsupported()

  override fun createSocket(host: InetAddress?, port: Int): Nothing = unsupported()

  override fun createSocket(
    address: InetAddress?,
    port: Int,
    localAddress: InetAddress?,
    localPort: Int,
  ): Nothing = unsupported()

  private fun unsupported(): Nothing =
    throw UnsupportedOperationException(
      "Destination is fixed to $socketPath; use the no-argument createSocket()"
    )
}

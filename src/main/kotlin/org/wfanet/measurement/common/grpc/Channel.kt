// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.NettyChannelBuilder
import java.security.cert.X509Certificate
import java.time.Duration
import org.wfanet.measurement.common.crypto.SigningCerts

/**
 * Builds a [ManagedChannel] for the specified target.
 *
 * @param target the URI or authority string for the target server
 */
fun buildPlaintextChannel(target: String): ManagedChannel {
  return ManagedChannelBuilder.forTarget(target)
    // TODO: Add assertion to ensure it's never called in a prod env
    .usePlaintext()
    .build()
}

/**
 * Builds a [ManagedChannel] for the specified target with mTLS connection.
 *
 * @param target the URI or authority string for the target server.
 * @param clientCerts the collection of client certificate and private key, as well as the trusted
 * server certificates.
 * @param hostName the expected DNS hostname from the Subject Alternative Name (SAN) of the server's
 * certificate
 */
fun buildMutualTlsChannel(
  target: String,
  clientCerts: SigningCerts,
  hostName: String? = null
): ManagedChannel {
  val channelBuilder =
    NettyChannelBuilder.forTarget(target).sslContext(clientCerts.toClientTlsContext())
  return if (hostName == null) {
    channelBuilder.build()
  } else {
    channelBuilder.overrideAuthority(hostName).build()
  }
}

/**
 * Builds a [ManagedChannel] for the specified target with TLS connection.
 *
 * @param target the URI or authority string for the target server
 * @param trustedServerCerts trusted server certificates.
 * @param hostName the expected DNS hostname from the Subject Alternative Name (SAN) of the server's
 * certificate
 */
fun buildTlsChannel(
  target: String,
  trustedServerCerts: Collection<X509Certificate>,
  hostName: String? = null
): ManagedChannel {
  val channelBuilder =
    NettyChannelBuilder.forTarget(target).sslContext(trustedServerCerts.toClientTlsContext())
  return if (hostName == null) {
    channelBuilder.build()
  } else {
    channelBuilder.overrideAuthority(hostName).build()
  }
}

/** Add shutdownHook to a managedChannel */
fun ManagedChannel.withShutdownTimeout(shutdownTimeout: Duration): ManagedChannel {
  return this.also { Runtime.getRuntime().addShutdownHook(it, shutdownTimeout) }
}

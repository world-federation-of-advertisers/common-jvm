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

import com.google.protobuf.util.Durations
import com.google.rpc.Code
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.grpc.serviceconfig.MethodConfigKt
import io.grpc.serviceconfig.methodConfig
import io.grpc.serviceconfig.serviceConfig
import java.security.cert.X509Certificate
import java.time.Duration
import org.wfanet.measurement.common.crypto.SigningCerts

/**
 * Builds a [ManagedChannel] for the specified target with mTLS connection.
 *
 * @param target the URI or authority string for the target server.
 * @param clientCerts the collection of client certificate and private key, as well as the trusted
 *   server certificates.
 * @param hostName the expected DNS hostname from the Subject Alternative Name (SAN) of the server's
 *   certificate
 * @param defaultServiceConfig [ServiceConfig] to use when none is provided by the name resolver.
 */
fun buildMutualTlsChannel(
  target: String,
  clientCerts: SigningCerts,
  hostName: String? = null,
  defaultServiceConfig: ServiceConfig = DEFAULT_SERVICE_CONFIG,
): ManagedChannel {
  return NettyChannelBuilder.forTarget(target)
    .apply {
      directExecutor() // See https://github.com/grpc/grpc-kotlin/issues/263
      sslContext(clientCerts.toClientTlsContext())
      if (hostName != null) {
        overrideAuthority(hostName)
      }
      defaultServiceConfig(defaultServiceConfig.asMap())
    }
    .build()
}

/**
 * Builds a [ManagedChannel] for the specified target with TLS connection.
 *
 * @param target the URI or authority string for the target server
 * @param trustedServerCerts trusted server certificates.
 * @param hostName the expected DNS hostname from the Subject Alternative Name (SAN) of the server's
 *   certificate
 * @param defaultServiceConfig [ServiceConfig] to use when none is provided by the name resolver.
 */
fun buildTlsChannel(
  target: String,
  trustedServerCerts: Collection<X509Certificate>,
  hostName: String? = null,
  defaultServiceConfig: ServiceConfig = DEFAULT_SERVICE_CONFIG,
): ManagedChannel {
  return NettyChannelBuilder.forTarget(target)
    .apply {
      directExecutor() // See https://github.com/grpc/grpc-kotlin/issues/263
      sslContext(trustedServerCerts.toClientTlsContext())
      if (hostName != null) {
        overrideAuthority(hostName)
      }
      defaultServiceConfig(defaultServiceConfig.asMap())
    }
    .build()
}

/** Add shutdownHook to a managedChannel */
fun ManagedChannel.withShutdownTimeout(shutdownTimeout: Duration): ManagedChannel {
  return this.also { Runtime.getRuntime().addShutdownHook(it, shutdownTimeout) }
}

private val DEFAULT_SERVICE_CONFIG =
  ProtobufServiceConfig(
    serviceConfig {
      methodConfig += methodConfig {
        timeout = Durations.fromSeconds(30)
        retryPolicy =
          MethodConfigKt.retryPolicy {
            retryableStatusCodes += Code.UNAVAILABLE
            maxAttempts = 10
            initialBackoff = Durations.fromMillis(100)
            maxBackoff = Durations.fromSeconds(1)
            backoffMultiplier = 1.5f
          }
      }
    }
  )

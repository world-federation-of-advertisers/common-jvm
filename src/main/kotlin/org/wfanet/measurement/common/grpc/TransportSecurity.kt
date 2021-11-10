// Copyright 2021 The Cross-Media Measurement Authors
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

import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle.Companion.keyManager

private const val TLS_V13_PROTOCOL = "TLSv1.3"

/**
 * Converts a collection of [X509Certificate]s into an [SslContext] for a gRPC client with only
 * server authentication (TLS).
 */
fun Collection<X509Certificate>.toClientTlsContext(): SslContext {
  return GrpcSslContexts.forClient().protocols(TLS_V13_PROTOCOL).trustManager(this).build()
}

/**
 * Converts this [SigningCerts] into an [SslContext] for a gRPC client with client authentication
 * (mTLS).
 */
fun SigningCerts.toClientTlsContext(): SslContext {
  return GrpcSslContexts.forClient()
    .protocols(TLS_V13_PROTOCOL)
    .keyManager(privateKeyHandle)
    .trustManager(trustedCertificates)
    .build()
}

/** Converts this [SigningCerts] into an [SslContext] for a gRPC server with TLS. */
fun SigningCerts.toServerTlsContext(clientAuth: ClientAuth = ClientAuth.NONE): SslContext {
  return GrpcSslContexts.configure(privateKeyHandle.newServerSslContextBuilder())
    .protocols(TLS_V13_PROTOCOL)
    .trustManager(trustedCertificates)
    .clientAuth(clientAuth)
    .build()
}

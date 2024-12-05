/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.grpc

import io.grpc.Context
import io.grpc.Grpc
import io.grpc.ServerCall
import io.grpc.Status
import java.security.cert.X509Certificate

object ClientCertificateAuthentication {
  val CLIENT_CERTIFICATE_CONTEXT_KEY: Context.Key<X509Certificate> =
    Context.key("client-certificate")

  /**
   * Extracts the TLS client certificate from [call].
   *
   * @return gRPC [Context] with [CLIENT_CERTIFICATE_CONTEXT_KEY] set
   * @throws io.grpc.StatusException on failure
   */
  fun <ReqT, RespT> extractClientCertificate(call: ServerCall<ReqT, RespT>): Context {
    val sslSession =
      call.attributes[Grpc.TRANSPORT_ATTR_SSL_SESSION]
        ?: throw Status.UNAUTHENTICATED.withDescription("No SSL session").asException()
    val clientCert =
      sslSession.peerCertificates.firstOrNull()
        ?: throw Status.UNAUTHENTICATED.withDescription("No client certificate").asException()
    return Context.current()
      .withValue(CLIENT_CERTIFICATE_CONTEXT_KEY, clientCert as X509Certificate)
  }
}

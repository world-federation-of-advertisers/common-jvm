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

import io.grpc.CallCredentials
import io.grpc.Metadata
import io.grpc.SecurityLevel
import io.grpc.Status
import java.util.concurrent.Executor

/**
 * [CallCredentials] for a bearer auth token.
 *
 * @param token the bearer token
 * @param requirePrivacy whether to require that the transport's security level is
 *   [SecurityLevel.PRIVACY_AND_INTEGRITY], e.g. that the transport is encrypted via TLS
 */
class BearerTokenCallCredentials(val token: String, private val requirePrivacy: Boolean = true) :
  CallCredentials() {
  override fun applyRequestMetadata(
    requestInfo: RequestInfo,
    appExecutor: Executor,
    applier: MetadataApplier,
  ) {
    if (requirePrivacy && requestInfo.securityLevel != SecurityLevel.PRIVACY_AND_INTEGRITY) {
      applier.fail(Status.UNAUTHENTICATED.withDescription("Credentials require private transport"))
    }

    val headers = Metadata()
    headers.put(AUTHORIZATION_METADATA_KEY, "$AUTH_TYPE $token")

    applier.apply(headers)
  }

  companion object {
    private val AUTHORIZATION_METADATA_KEY: Metadata.Key<String> =
      Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
    private const val AUTH_TYPE = "Bearer"

    fun fromHeaders(
      headers: Metadata,
      requirePrivacy: Boolean = true,
    ): BearerTokenCallCredentials? {
      val authHeader = headers[AUTHORIZATION_METADATA_KEY] ?: return null
      if (!authHeader.startsWith(AUTH_TYPE)) {
        return null
      }

      val token = authHeader.substring(AUTH_TYPE.length).trim()
      return BearerTokenCallCredentials(token, requirePrivacy)
    }
  }
}

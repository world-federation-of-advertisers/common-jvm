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

package org.wfanet.measurement.common.grpc.testing

import com.google.crypto.tink.KeyTemplate
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.jwt.JwkSetConverter
import com.google.crypto.tink.jwt.JwtPublicKeySign
import com.google.crypto.tink.jwt.JwtSignatureConfig
import com.google.crypto.tink.jwt.RawJwt
import java.time.Clock
import java.time.Duration
import java.util.UUID
import org.wfanet.measurement.common.grpc.BearerTokenCallCredentials
import org.wfanet.measurement.common.grpc.OAuthTokenAuthentication

/** An OpenID provider for testing. */
class OpenIdProvider(
  private val issuer: String,
  private val jwkSetHandle: KeysetHandle = KeysetHandle.generateNew(KEY_TEMPLATE),
  private val clock: Clock = Clock.systemUTC(),
  private val generateUuid: () -> UUID = UUID::randomUUID,
) {
  val providerConfig: OAuthTokenAuthentication.OpenIdProviderConfig by lazy {
    val jwks = JwkSetConverter.fromPublicKeysetHandle(jwkSetHandle.publicKeysetHandle)
    OAuthTokenAuthentication.OpenIdProviderConfig(issuer, jwks)
  }

  fun generateCredentials(
    audience: String,
    subject: String,
    scopes: Set<String>,
    ttl: Duration = Duration.ofMinutes(5),
    clientId: String = DEFAULT_CLIENT_ID,
  ): BearerTokenCallCredentials {
    val token = generateSignedToken(audience, subject, scopes, clientId, ttl)
    return BearerTokenCallCredentials(token, false)
  }

  /** Generates a signed and encoded JWT using the specified parameters. */
  private fun generateSignedToken(
    audience: String,
    subject: String,
    scopes: Set<String>,
    clientId: String,
    ttl: Duration,
  ): String {
    val jwtId = generateUuid()
    val issuedAt = clock.instant()
    val rawJwt =
      RawJwt.newBuilder()
        .setJwtId(jwtId.toString())
        .setAudience(audience)
        .setIssuer(issuer)
        .setSubject(subject)
        .setIssuedAt(issuedAt)
        .setExpiration(issuedAt.plus(ttl))
        .addStringClaim("client_id", clientId)
        .addStringClaim("scope", scopes.joinToString(" "))
        .build()
    val signer = jwkSetHandle.getPrimitive(JwtPublicKeySign::class.java)
    return signer.signAndEncode(rawJwt)
  }

  companion object {
    init {
      JwtSignatureConfig.register()
    }

    const val DEFAULT_CLIENT_ID = "testing-client"
    private val KEY_TEMPLATE: KeyTemplate = KeyTemplates.get("JWT_ES256")
  }
}

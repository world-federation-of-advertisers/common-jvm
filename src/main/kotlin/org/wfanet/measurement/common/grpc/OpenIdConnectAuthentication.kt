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

import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.jwt.JwkSetConverter
import com.google.crypto.tink.jwt.JwtPublicKeyVerify
import com.google.crypto.tink.jwt.JwtSignatureConfig
import com.google.crypto.tink.jwt.JwtValidator
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.grpc.Metadata
import io.grpc.Status
import java.io.IOException
import java.security.GeneralSecurityException
import java.time.Clock
import org.wfanet.measurement.common.base64UrlDecode

/** Utility for extracting OpenID Connect (OIDC) token information from gRPC request headers. */
class OpenIdConnectAuthentication(
  audience: String,
  openIdProviderConfigs: Iterable<OpenIdProviderConfig>,
  clock: Clock = Clock.systemUTC(),
) {
  private val jwtValidator =
    JwtValidator.newBuilder().setClock(clock).expectAudience(audience).ignoreIssuer().build()

  private val jwksHandleByIssuer: Map<String, KeysetHandle> =
    openIdProviderConfigs.associateBy({ it.issuer }) {
      JwkSetConverter.toPublicKeysetHandle(it.jwks)
    }

  /**
   * Verifies and decodes an OIDC bearer token from [headers].
   *
   * The token must be a signed JWT.
   *
   * @throws io.grpc.StatusException on failure
   */
  fun verifyAndDecodeBearerToken(headers: Metadata): VerifiedToken {
    val credentials =
      BearerTokenCallCredentials.fromHeaders(headers)
        ?: throw Status.UNAUTHENTICATED.withDescription("Bearer token not found in headers")
          .asException()

    val token: String = credentials.token
    val tokenParts = token.split(".")
    if (tokenParts.size != 3) {
      throw Status.UNAUTHENTICATED.withDescription("Token is not a valid signed JWT").asException()
    }
    val payload: JsonObject =
      try {
        JsonParser.parseString(tokenParts[1].base64UrlDecode().toStringUtf8()).asJsonObject
      } catch (e: IOException) {
        throw Status.UNAUTHENTICATED.withCause(e)
          .withDescription("Token is not a valid signed JWT")
          .asException()
      }

    val issuer =
      payload.get(ISSUER_CLAIM)?.asString
        ?: throw Status.UNAUTHENTICATED.withDescription("Issuer not found").asException()
    val jwksHandle =
      jwksHandleByIssuer[issuer]
        ?: throw Status.UNAUTHENTICATED.withDescription("Unknown issuer").asException()

    val verifiedJwt =
      try {
        jwksHandle.getPrimitive(JwtPublicKeyVerify::class.java).verifyAndDecode(token, jwtValidator)
      } catch (e: GeneralSecurityException) {
        throw Status.UNAUTHENTICATED.withCause(e).withDescription(e.message).asException()
      }

    if (!verifiedJwt.hasSubject()) {
      throw Status.UNAUTHENTICATED.withDescription("Subject not found").asException()
    }
    val scopes: Set<String> =
      if (verifiedJwt.hasStringClaim(SCOPES_CLAIM)) {
        verifiedJwt.getStringClaim(SCOPES_CLAIM).split(" ").toSet()
      } else {
        emptySet()
      }

    return VerifiedToken(issuer, verifiedJwt.subject, scopes)
  }

  companion object {
    init {
      JwtSignatureConfig.register()
    }

    private const val ISSUER_CLAIM = "iss"
    private const val SCOPES_CLAIM = "scope"
  }

  data class VerifiedToken(val issuer: String, val subject: String, val scopes: Set<String>)

  data class OpenIdProviderConfig(
    val issuer: String,
    /** JSON Web Key Set (JWKS) for the provider. */
    val jwks: String,
  )
}

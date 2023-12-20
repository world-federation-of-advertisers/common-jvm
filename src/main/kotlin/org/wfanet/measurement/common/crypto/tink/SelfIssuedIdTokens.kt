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

/*
 * Contains methods for working with self-issued ID tokens.
 */

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.jwt.JwtValidator
import com.google.crypto.tink.jwt.RawJwt
import com.google.gson.JsonObject
import com.google.protobuf.kotlin.toByteStringUtf8
import java.net.URI
import java.security.GeneralSecurityException
import java.time.Clock
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.hashSha256

private const val EXPIRATION_SECONDS = 5000L

object SelfIssuedIdTokens {
  private const val STATE = "state"
  private const val NONCE = "nonce"
  private const val SELF_ISSUED_ISSUER = "https://self-issued.me"
  private const val OPEN_ID_SCHEME = "openid"
  private val HEADER: String =
    JsonObject()
      .apply {
        addProperty("typ", "JWT")
        addProperty("alg", "RS256")
      }
      .toString()

  /**
   * Returns a self-issued id token using a generated private key.
   *
   * @throws IllegalArgumentException if the uriString doesn't match the open id connect
   *   requirements for self-issued, or doesn't include state and nonce.
   */
  fun generateIdToken(uriString: String, clock: Clock): String {
    return generateIdToken(PrivateJwkHandle.generateRsa(), uriString, clock)
  }

  /**
   * Returns a self-issued id token using a provided private key.
   *
   * @throws IllegalArgumentException if the uriString doesn't match the open id connect
   *   requirements for self-issued, or doesn't include state and nonce.
   */
  fun generateIdToken(privateJwkHandle: PrivateJwkHandle, uriString: String, clock: Clock): String {
    val uri = URI.create(uriString)

    require(uri.scheme.equals(OPEN_ID_SCHEME)) {
      "Invalid scheme for Self-Issued OpenID Provider: ${uri.scheme}"
    }

    val queryParamMap = buildQueryParamMap(uri)
    if (!isQueryValid(queryParamMap)) {
      throw IllegalArgumentException("URI query parameters are invalid")
    }

    val jwk = privateJwkHandle.publicKey.getJwk()
    val now = clock.instant()

    return privateJwkHandle.sign(
      RawJwt.newBuilder()
        .setIssuer(SELF_ISSUED_ISSUER)
        .setSubject(calculateRsaThumbprint(jwk.toString()))
        .addAudience(queryParamMap["client_id"])
        .setTypeHeader(HEADER)
        .setExpiration(now.plusSeconds(EXPIRATION_SECONDS))
        .setIssuedAt(now)
        .addJsonObjectClaim("sub_jwk", jwk.toString())
        .addStringClaim(STATE, queryParamMap[STATE])
        .addStringClaim(NONCE, queryParamMap[NONCE])
        .build()
    )
  }

  private fun buildQueryParamMap(uri: URI): Map<String, String> {
    val queryParamMap = mutableMapOf<String, String>()

    for (queryParam in uri.query.split("&")) {
      val keyValue = queryParam.split("=")
      queryParamMap[keyValue[0]] = keyValue[1]
    }

    return queryParamMap
  }

  private fun isQueryValid(queryParamMap: Map<String, String>): Boolean {
    return queryParamMap.getOrDefault("scope", "").contains("openid") &&
      queryParamMap.getOrDefault("response_type", "") == "id_token" &&
      queryParamMap.contains(STATE) &&
      queryParamMap.contains(NONCE)
  }

  fun calculateRsaThumbprint(jwk: String): String {
    val hash = Hashing.hashSha256(jwk.toByteStringUtf8())
    return hash.base64UrlEncode()
  }

  /**
   * Validates the signature, the header, and the following claims: issuer and audience.
   *
   * @throws GeneralSecurityException if the validation fails
   */
  fun validateJwt(
    idToken: String,
    publicJwkHandle: PublicJwkHandle,
    redirectUri: String
  ): VerifiedJwt {
    val validator =
      JwtValidator.newBuilder()
        .expectIssuer(SELF_ISSUED_ISSUER)
        .expectAudience(redirectUri)
        .expectTypeHeader(HEADER)
        .build()

    return publicJwkHandle.verifyAndDecode(idToken, validator)
  }
}

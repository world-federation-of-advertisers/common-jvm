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
import java.time.Duration
import java.time.Instant
import org.wfanet.measurement.common.grpc.BearerTokenCallCredentials
import org.wfanet.measurement.common.grpc.OpenIdConnectAuthentication

/** An ephemeral OpenID provider for testing. */
class OpenIdProvider(issuer: String, clientId: String) {
  private val jwkSetHandle = KeysetHandle.generateNew(KEY_TEMPLATE)

  val providerConfig =
    OpenIdConnectAuthentication.OpenIdProviderConfig(
      issuer,
      JwkSetConverter.fromPublicKeysetHandle(jwkSetHandle.publicKeysetHandle),
      clientId,
    )

  fun generateCredentials(
    subject: String,
    scopes: Set<String>,
    expiration: Instant = Instant.now().plus(Duration.ofMinutes(5)),
  ): BearerTokenCallCredentials {
    val token = generateSignedToken(subject, scopes, expiration)
    return BearerTokenCallCredentials(token, false)
  }

  /** Generates a signed and encoded JWT using the specified parameters. */
  private fun generateSignedToken(
    subject: String,
    scopes: Set<String>,
    expiration: Instant,
  ): String {
    val rawJwt =
      RawJwt.newBuilder()
        .setAudience(providerConfig.clientId)
        .setIssuer(providerConfig.issuer)
        .setSubject(subject)
        .addStringClaim("scope", scopes.joinToString(" "))
        .setExpiration(expiration)
        .build()
    val signer = jwkSetHandle.getPrimitive(JwtPublicKeySign::class.java)
    return signer.signAndEncode(rawJwt)
  }

  companion object {
    init {
      JwtSignatureConfig.register()
    }

    private val KEY_TEMPLATE: KeyTemplate = KeyTemplates.get("JWT_ES256")
  }
}

/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import com.google.gson.JsonParser
import java.net.URLEncoder
import java.time.Clock
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.openid.createRequestUri

private const val SCHEME = "openid"
private const val STATE = 5L
private const val NONCE = 5L
private const val SCOPE = "openid"
private const val REDIRECT_URI = "https://redirect:2048"

@RunWith(JUnit4::class)
class SelfIssuedIdTokensTest {
  private val clock: Clock = Clock.systemUTC()

  @Test
  fun `Self issued id token generation is successful`() {
    val idToken =
      SelfIssuedIdTokens.generateIdToken(
        createRequestUri(
          state = STATE,
          nonce = NONCE,
          redirectUri = REDIRECT_URI,
          isSelfIssued = true,
        ),
        clock,
      )
    val tokenParts = idToken.split(".")
    val claims =
      JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject
    val subJwk = claims.get("sub_jwk")
    val jwk = subJwk.asJsonObject
    val publicJwkHandle = PublicJwkHandle.fromJwk(jwk)
    val verifiedJwt =
      SelfIssuedIdTokens.validateJwt(
        redirectUri = REDIRECT_URI,
        idToken = idToken,
        publicJwkHandle = publicJwkHandle,
      )

    assertThat(apiIdToExternalId(claims.get("state").asString) == STATE).isTrue()
    assertThat(apiIdToExternalId(claims.get("nonce").asString) == NONCE).isTrue()
    assertThat(
        verifiedJwt.subject.equals(SelfIssuedIdTokens.calculateRsaThumbprint(jwk.toString()))
      )
      .isTrue()
  }

  @Test
  fun `Self issued id token generation fails with wrong scheme`() {
    assertFailsWith<IllegalArgumentException> {
      SelfIssuedIdTokens.generateIdToken(generateRequestUri("http", STATE, NONCE), clock)
    }
  }

  @Test
  fun `Self issued id token generation fails with missing scope`() {
    assertFailsWith<IllegalArgumentException> {
      SelfIssuedIdTokens.generateIdToken(generateRequestUri(SCHEME, STATE, NONCE), clock)
    }
  }

  @Test
  fun `Self issued id token generation fails with wrong scope`() {
    assertFailsWith<IllegalArgumentException> {
      SelfIssuedIdTokens.generateIdToken(generateRequestUri(SCHEME, STATE, NONCE, "test"), clock)
    }
  }

  @Test
  fun `Self issued id token generation fails with missing state`() {
    assertFailsWith<IllegalArgumentException> {
      SelfIssuedIdTokens.generateIdToken(generateRequestUri(SCHEME, null, NONCE, SCOPE), clock)
    }
  }

  companion object {
    private fun generateRequestUri(
      scheme: String,
      state: Long?,
      nonce: Long,
      scope: String = "",
    ): String {
      val uriParts = mutableListOf<String>()
      uriParts.add("$scheme://?response_type=id_token")
      uriParts.add("scope=$scope")
      if (state != null) {
        uriParts.add("state=" + externalIdToApiId(state))
      }
      uriParts.add("nonce=" + externalIdToApiId(nonce))
      val redirectUri = URLEncoder.encode(REDIRECT_URI, "UTF-8")
      uriParts.add("client_id=$redirectUri")

      return uriParts.joinToString("&")
    }
  }
}

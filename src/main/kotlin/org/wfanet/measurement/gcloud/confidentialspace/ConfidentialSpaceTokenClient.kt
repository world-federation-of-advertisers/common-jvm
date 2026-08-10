// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.confidentialspace

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.netty.channel.unix.DomainSocketAddress
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.Base64
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.reactive.awaitSingle
import reactor.core.publisher.Mono
import reactor.netty.ByteBufMono
import reactor.netty.http.client.HttpClient

/** Attestation-token type understood by the Confidential Space launcher token endpoint. */
enum class ConfidentialSpaceTokenType(val wireValue: String) {
  /** Standard OIDC token, validated against the issuer's rotating public keys. */
  OIDC("OIDC"),
  /** Token validated against a long-lived root certificate chain. */
  PKI("PKI"),
  /** Token whose attestation claims are exposed to AWS IAM as principal (session) tags. */
  AWS_PRINCIPAL_TAGS("AWS_PRINCIPALTAGS"),
}

/**
 * Request for a custom Confidential Space attestation token.
 *
 * @param audience The audience baked into the token; echoed back in the `aud` claim.
 * @param tokenType The type of token to mint.
 * @param nonces Optional nonces echoed into the token (e.g. for channel binding).
 */
data class AttestationTokenRequest(
  val audience: String,
  val tokenType: ConfidentialSpaceTokenType,
  val nonces: List<String> = emptyList(),
  /**
   * Container image signature key IDs to surface as the `container.signatures.key_id` AWS principal
   * tag (used by AWS_PRINCIPALTAGS tokens). Empty means no signature tag is requested, in which
   * case the token instead carries `container.image_digest`.
   */
  val containerImageSignatureKeyIds: List<String> = emptyList(),
)

/** Obtains Confidential Space attestation tokens. */
fun interface AttestationTokenProvider {
  /**
   * Returns a freshly minted attestation token for [request].
   *
   * @throws IOException if the token cannot be obtained.
   */
  suspend fun getToken(request: AttestationTokenRequest): String
}

/**
 * Client for the Confidential Space launcher's local attestation-token service.
 *
 * The launcher exposes an HTTP endpoint over a Unix domain socket at [socketPath]. Requests are
 * issued by Reactor Netty over a [DomainSocketAddress], which requires the Netty native transport
 * for the host platform.
 *
 * @param socketPath Filesystem path of the launcher token socket.
 * @param requestTimeout Bound on a token request, covering both connecting to the socket and
 *   awaiting the response.
 */
class ConfidentialSpaceTokenClient(
  private val socketPath: Path = Paths.get(DEFAULT_SOCKET_PATH),
  private val requestTimeout: Duration = DEFAULT_REQUEST_TIMEOUT,
) : AttestationTokenProvider {

  private val httpClient: HttpClient =
    HttpClient.create()
      .remoteAddress { DomainSocketAddress(socketPath.toString()) }
      .responseTimeout(requestTimeout)
      .headers { headers ->
        headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
      }

  override suspend fun getToken(request: AttestationTokenRequest): String {
    val requestBody = buildRequestBody(request)
    val contentLength = requestBody.toByteArray(StandardCharsets.UTF_8).size
    val response: TokenResponse =
      try {
        httpClient
          .headers { headers -> headers.set(HttpHeaderNames.CONTENT_LENGTH, contentLength) }
          .post()
          .uri(TOKEN_PATH)
          .send(ByteBufMono.fromString(Mono.just(requestBody)))
          .responseSingle { httpResponse, body ->
            body.asString(StandardCharsets.UTF_8).defaultIfEmpty("").map { bodyText ->
              TokenResponse(httpResponse.status().code(), bodyText)
            }
          }
          .awaitSingle()
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        throw IOException("Launcher token request to $socketPath failed", e)
      }

    val token = response.body.trim()
    if (response.statusCode !in SUCCESS_STATUS_CODES) {
      throw IOException(
        "Launcher token request failed with status ${response.statusCode}: ${redactIfJwt(token)}"
      )
    }
    if (token.isEmpty()) {
      throw IOException("Launcher returned an empty attestation token")
    }
    return token
  }

  private fun buildRequestBody(request: AttestationTokenRequest): String =
    JsonObject()
      .apply {
        addProperty("audience", request.audience)
        addProperty("token_type", request.tokenType.wireValue)
        if (request.nonces.isNotEmpty()) {
          add("nonces", JsonArray().apply { request.nonces.forEach { add(it) } })
        }
        if (request.tokenType == ConfidentialSpaceTokenType.AWS_PRINCIPAL_TAGS) {
          // key_ids surfaces the container image signature key IDs as the
          // container.signatures.key_id principal tag; an empty list yields
          // container.image_digest instead.
          val keyIds =
            JsonArray().apply { request.containerImageSignatureKeyIds.forEach { add(it) } }
          val containerImageSignatures = JsonObject().apply { add("key_ids", keyIds) }
          val allowedPrincipalTags =
            JsonObject().apply { add("container_image_signatures", containerImageSignatures) }
          val awsPrincipalTagOptions =
            JsonObject().apply { add("allowed_principal_tags", allowedPrincipalTags) }
          // aws_principal_tag_options is sent even when no key IDs were requested: without it the
          // launcher returns an empty response (google/go-tpm-tools#770).
          add("aws_principal_tag_options", awsPrincipalTagOptions)
        }
      }
      .toString()

  /** Status code and body of a launcher token response. */
  private data class TokenResponse(val statusCode: Int, val body: String)

  companion object {
    /** Default Unix domain socket exposed by the Confidential Space launcher. */
    const val DEFAULT_SOCKET_PATH = "/run/container_launcher/teeserver.sock"

    /** Path of the launcher token endpoint. */
    const val TOKEN_PATH = "/v1/token"

    private val DEFAULT_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(30)

    private val SUCCESS_STATUS_CODES = 200..299

    /**
     * Extracts container image signature key IDs from a Confidential Space attestation [token] by
     * reading its `submods.container.image_signatures[].key_id` claims. Lets callers self-discover
     * which signature key IDs to surface as AWS principal tags, so no key IDs need to be hardcoded.
     * The AWS role trust policy remains the authority on which signers are acceptable.
     *
     * @throws IllegalArgumentException if [token] is not a JWT.
     */
    fun parseContainerImageSignatureKeyIds(token: String): List<String> {
      val parts = token.split(".")
      require(parts.size >= 2) { "Malformed JWT attestation token" }
      val payload =
        String(Base64.getUrlDecoder().decode(padBase64(parts[1])), StandardCharsets.UTF_8)
      val claims =
        try {
          JsonParser.parseString(payload).asJsonObject
        } catch (e: RuntimeException) {
          throw IllegalArgumentException("Malformed JWT attestation token payload", e)
        }
      val signatures =
        claims
          .getAsJsonObject("submods")
          ?.getAsJsonObject("container")
          ?.getAsJsonArray("image_signatures")
      return signatures?.mapNotNull { it.asJsonObject.get("key_id")?.asString }?.distinct()
        ?: emptyList()
    }

    private fun padBase64(value: String): String {
      val remainder = value.length % 4
      return if (remainder == 0) value else value + "=".repeat(4 - remainder)
    }

    /**
     * Redacts [value] if it looks like a JWT (three non-empty base64url segments), so a bearer
     * token is never written to logs; other bodies are returned unchanged for debugging.
     */
    private fun redactIfJwt(value: String): String {
      val parts = value.split(".")
      val looksLikeJwt =
        parts.size == 3 &&
          parts.all { part ->
            part.isNotEmpty() && part.all { it.isLetterOrDigit() || it == '_' || it == '-' }
          }
      return if (looksLikeJwt) "[redacted possible JWT]" else value
    }
  }
}

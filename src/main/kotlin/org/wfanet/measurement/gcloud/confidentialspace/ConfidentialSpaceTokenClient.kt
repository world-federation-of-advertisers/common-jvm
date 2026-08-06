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
import java.io.IOException
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.Base64
import okhttp3.Dns
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.wfanet.measurement.common.net.UnixDomainSocketFactory

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
  /** Returns a freshly minted attestation token for [request]. */
  fun getToken(request: AttestationTokenRequest): String
}

/**
 * Client for the Confidential Space launcher's local attestation-token service.
 *
 * The launcher exposes an HTTP endpoint over a Unix domain socket at [socketPath] (default
 * [DEFAULT_SOCKET_PATH]). Requests go through OkHttp over a [UnixDomainSocketFactory], so response
 * framing (`Transfer-Encoding: chunked`, `Content-Length`) is handled by the HTTP client rather
 * than by hand.
 *
 * Unix domain sockets require Java 16 or later.
 *
 * @param socketPath Filesystem path of the launcher token socket.
 * @param requestTimeout Overall bound on a token request, covering both connecting to the socket
 *   and awaiting the response.
 */
class ConfidentialSpaceTokenClient(
  private val socketPath: Path = Paths.get(DEFAULT_SOCKET_PATH),
  private val requestTimeout: Duration = DEFAULT_REQUEST_TIMEOUT,
) : AttestationTokenProvider {

  private val httpClient: OkHttpClient by lazy {
    OkHttpClient.Builder()
      .socketFactory(UnixDomainSocketFactory(socketPath))
      // The URL host is a placeholder for a socket path, so resolving it must not hit DNS.
      .dns(LOOPBACK_DNS)
      .connectTimeout(requestTimeout)
      .readTimeout(requestTimeout)
      .writeTimeout(requestTimeout)
      .build()
  }

  override fun getToken(request: AttestationTokenRequest): String {
    val httpRequest =
      Request.Builder()
        .url(TOKEN_URL)
        .post(buildRequestBody(request).toRequestBody(JSON_MEDIA_TYPE))
        .build()

    httpClient.newCall(httpRequest).execute().use { response ->
      val token = response.body?.string().orEmpty().trim()
      if (!response.isSuccessful) {
        throw IOException(
          "Launcher token request failed: ${response.code} ${response.message}; " +
            "body=${redactIfJwt(token)}"
        )
      }
      if (token.isEmpty()) {
        throw IOException("Launcher returned an empty attestation token")
      }
      return token
    }
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
          // For AWS_PRINCIPALTAGS the launcher reads aws_principal_tag_options unconditionally.
          // Launcher builds predating the nil guard in go-tpm-tools convertToCSOpts panic (nil
          // pointer dereference) when it is absent and write a zero-byte body, which surfaces here
          // as a malformed/empty response. Sending the full structure keeps
          // TokenOptions.token_type_options non-nil on every launcher version. key_ids carries the
          // container image signature key IDs to surface as the container.signatures.key_id
          // principal tag; when empty the token instead carries container.image_digest.
          val keyIds =
            JsonArray().apply { request.containerImageSignatureKeyIds.forEach { add(it) } }
          val containerImageSignatures = JsonObject().apply { add("key_ids", keyIds) }
          val allowedPrincipalTags =
            JsonObject().apply { add("container_image_signatures", containerImageSignatures) }
          val awsPrincipalTagOptions =
            JsonObject().apply { add("allowed_principal_tags", allowedPrincipalTags) }
          add("aws_principal_tag_options", awsPrincipalTagOptions)
        }
      }
      .toString()

  companion object {
    /** Default Unix domain socket exposed by the Confidential Space launcher. */
    const val DEFAULT_SOCKET_PATH = "/run/container_launcher/teeserver.sock"
    /** Path of the launcher token endpoint. */
    const val TOKEN_PATH = "/v1/token"

    /** The host is ignored; the socket factory determines the destination. */
    private const val TOKEN_URL = "http://localhost$TOKEN_PATH"

    private val DEFAULT_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(30)
    private val JSON_MEDIA_TYPE = "application/json; charset=utf-8".toMediaType()
    private val LOOPBACK_DNS =
      object : Dns {
        override fun lookup(hostname: String): List<InetAddress> =
          listOf(InetAddress.getLoopbackAddress())
      }

    /**
     * Extracts container image signature key IDs from a Confidential Space attestation [token] by
     * reading its `submods.container.image_signatures[].key_id` claims. Lets callers self-discover
     * which signature key IDs to surface as AWS principal tags, so no key IDs need to be hardcoded.
     * The AWS role trust policy remains the authority on which signers are acceptable.
     */
    fun parseContainerImageSignatureKeyIds(token: String): List<String> {
      val parts = token.split(".")
      require(parts.size >= 2) { "Malformed JWT attestation token" }
      val payload =
        String(Base64.getUrlDecoder().decode(padBase64(parts[1])), StandardCharsets.UTF_8)
      val signatures =
        JsonParser.parseString(payload)
          .asJsonObject
          .getAsJsonObject("submods")
          ?.getAsJsonObject("container")
          ?.getAsJsonArray("image_signatures")
      val keyIds =
        signatures?.mapNotNull { it.asJsonObject.get("key_id")?.asString }?.distinct()
          ?: emptyList()
      return keyIds
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

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
import java.io.IOException
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import org.newsclub.net.unix.AFUNIXSocket
import org.newsclub.net.unix.AFUNIXSocketAddress

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
 * [DEFAULT_SOCKET_PATH]). Because the JVM baseline is Java 11 — which has no built-in `AF_UNIX`
 * support — the connection is made via junixsocket. One request/response is exchanged per call with
 * `Connection: close`, so the response body is read to end-of-stream.
 *
 * @param socketPath Filesystem path of the launcher token socket.
 * @param readTimeout Socket read timeout for a token request.
 * @param socketFactory Opens a connected socket to [socketPath]; overridable for testing.
 */
class ConfidentialSpaceTokenClient(
  private val socketPath: Path = Paths.get(DEFAULT_SOCKET_PATH),
  private val readTimeout: Duration = DEFAULT_READ_TIMEOUT,
  private val socketFactory: (Path) -> Socket = ::connectUnixDomainSocket,
) : AttestationTokenProvider {

  override fun getToken(request: AttestationTokenRequest): String {
    val body: String =
      JsonObject()
        .apply {
          addProperty("audience", request.audience)
          addProperty("token_type", request.tokenType.wireValue)
          if (request.nonces.isNotEmpty()) {
            add("nonces", JsonArray().apply { request.nonces.forEach { add(it) } })
          }
        }
        .toString()

    val responseBytes: ByteArray =
      socketFactory(socketPath).use { socket ->
        socket.soTimeout = readTimeout.toMillis().toInt()
        val bodyBytes = body.toByteArray(StandardCharsets.UTF_8)
        val httpRequest = buildString {
          append("POST ").append(TOKEN_PATH).append(" HTTP/1.1\r\n")
          append("Host: localhost\r\n")
          append("Content-Type: application/json\r\n")
          append("Content-Length: ").append(bodyBytes.size).append("\r\n")
          append("Connection: close\r\n")
          append("\r\n")
        }
        socket.getOutputStream().apply {
          write(httpRequest.toByteArray(StandardCharsets.US_ASCII))
          write(bodyBytes)
          flush()
        }
        socket.getInputStream().readBytes()
      }

    return parseTokenResponse(responseBytes)
  }

  companion object {
    /** Default Unix domain socket exposed by the Confidential Space launcher. */
    const val DEFAULT_SOCKET_PATH = "/run/container_launcher/teeserver.sock"
    /** Path of the launcher token endpoint. */
    const val TOKEN_PATH = "/v1/token"

    private val DEFAULT_READ_TIMEOUT: Duration = Duration.ofSeconds(30)
    private const val HEADER_BODY_SEPARATOR = "\r\n\r\n"

    private fun connectUnixDomainSocket(path: Path): Socket =
      AFUNIXSocket.connectTo(AFUNIXSocketAddress.of(path.toFile()))

    private fun parseTokenResponse(responseBytes: ByteArray): String {
      val response = String(responseBytes, StandardCharsets.UTF_8)
      val separatorIndex = response.indexOf(HEADER_BODY_SEPARATOR)
      require(separatorIndex >= 0) { "Malformed HTTP response from launcher token socket" }
      val statusLine = response.substringBefore("\r\n")
      val statusCode = statusLine.split(" ").getOrNull(1)?.toIntOrNull()
      val tokenBody = response.substring(separatorIndex + HEADER_BODY_SEPARATOR.length).trim()
      if (statusCode != 200) {
        throw IOException("Launcher token request failed: $statusLine; body=$tokenBody")
      }
      check(tokenBody.isNotEmpty()) { "Launcher returned an empty attestation token" }
      return tokenBody
    }
  }
}

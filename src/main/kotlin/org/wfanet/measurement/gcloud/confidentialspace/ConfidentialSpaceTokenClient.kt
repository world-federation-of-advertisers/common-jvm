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
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.logging.Logger
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
 * `Connection: close`.
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

    // DO_NOT_SUBMIT(halo): verbose diagnostic logging of the raw launcher response, to pin down the
    // exact framing that breaks parseTokenResponse. Logged BEFORE parsing so we capture it even
    // when
    // parsing throws. Remove once the token-fetch bug is understood and fixed.
    logRawResponseForDebug(request, body, responseBytes)

    return parseTokenResponse(responseBytes)
  }

  /**
   * DO_NOT_SUBMIT(halo): diagnostic only — dumps the raw launcher reply so we can see its framing.
   */
  private fun logRawResponseForDebug(
    request: AttestationTokenRequest,
    requestBody: String,
    responseBytes: ByteArray,
  ) {
    val text = String(responseBytes, StandardCharsets.UTF_8)
    val escaped = text.take(2000).replace("\r", "<CR>").replace("\n", "<LF>")
    val hexPrefix = responseBytes.take(96).joinToString(" ") { "%02x".format(it.toInt() and 0xff) }
    logger.warning(
      "CS-TOKEN-DEBUG: tokenType=${request.tokenType.wireValue} audience=${request.audience} " +
        "socket=$socketPath requestBody=$requestBody | responseByteCount=${responseBytes.size} " +
        "hasCRLFCRLF=${indexOf(responseBytes, HEADER_BODY_SEPARATOR, 0) >= 0} " +
        "hasLFLF=${text.contains("\n\n")} | rawEscaped=[$escaped] | hex96=[$hexPrefix]"
    )
  }

  companion object {
    /** Default Unix domain socket exposed by the Confidential Space launcher. */
    const val DEFAULT_SOCKET_PATH = "/run/container_launcher/teeserver.sock"
    /** Path of the launcher token endpoint. */
    const val TOKEN_PATH = "/v1/token"

    private val DEFAULT_READ_TIMEOUT: Duration = Duration.ofSeconds(30)
    private val logger: Logger = Logger.getLogger(ConfidentialSpaceTokenClient::class.java.name)
    private val CRLF = byteArrayOf('\r'.code.toByte(), '\n'.code.toByte())
    private val HEADER_BODY_SEPARATOR = CRLF + CRLF

    private fun connectUnixDomainSocket(path: Path): Socket =
      AFUNIXSocket.connectTo(AFUNIXSocketAddress.of(path.toFile()))

    /**
     * Parses an HTTP/1.1 response from the launcher and returns the token body.
     *
     * Honors `Transfer-Encoding: chunked` and `Content-Length`, falling back to the bytes read
     * until end-of-stream otherwise. The launcher sets no `Content-Length`, and tokens larger than
     * the server's write buffer are sent chunked, so de-chunking is required for real tokens.
     */
    private fun parseTokenResponse(responseBytes: ByteArray): String {
      val headerEnd = indexOf(responseBytes, HEADER_BODY_SEPARATOR, 0)
      require(headerEnd >= 0) { "Malformed HTTP response from launcher token socket" }

      val headerLines = String(responseBytes, 0, headerEnd, StandardCharsets.US_ASCII).split("\r\n")
      val statusLine = headerLines.first()
      val statusCode = statusLine.split(" ").getOrNull(1)?.toIntOrNull()
      val headers: Map<String, String> =
        headerLines
          .drop(1)
          .mapNotNull { line ->
            val colon = line.indexOf(':')
            if (colon < 0) null
            else line.substring(0, colon).trim().lowercase() to line.substring(colon + 1).trim()
          }
          .toMap()

      val rawBody =
        responseBytes.copyOfRange(headerEnd + HEADER_BODY_SEPARATOR.size, responseBytes.size)
      val bodyBytes: ByteArray =
        if (headers["transfer-encoding"]?.contains("chunked", ignoreCase = true) == true) {
          decodeChunkedBody(rawBody)
        } else {
          val contentLength = headers["content-length"]?.toIntOrNull()
          if (contentLength != null) rawBody.copyOfRange(0, minOf(contentLength, rawBody.size))
          else rawBody
        }

      val token = String(bodyBytes, StandardCharsets.UTF_8).trim()
      if (statusCode == null || statusCode !in 200..299) {
        throw IOException("Launcher token request failed: $statusLine; body=$token")
      }
      check(token.isNotEmpty()) { "Launcher returned an empty attestation token" }
      return token
    }

    /** Decodes an HTTP `Transfer-Encoding: chunked` message body. */
    private fun decodeChunkedBody(body: ByteArray): ByteArray {
      val decoded = ByteArrayOutputStream()
      var position = 0
      while (position < body.size) {
        val lineEnd = indexOf(body, CRLF, position)
        if (lineEnd < 0) break
        val sizeToken =
          String(body, position, lineEnd - position, StandardCharsets.US_ASCII)
            .substringBefore(';')
            .trim()
        val chunkSize = sizeToken.toIntOrNull(16) ?: break
        position = lineEnd + CRLF.size
        if (chunkSize == 0) break
        val chunkEnd = minOf(position + chunkSize, body.size)
        decoded.write(body, position, chunkEnd - position)
        position = chunkEnd + CRLF.size
      }
      return decoded.toByteArray()
    }

    /** Returns the index of [needle] in [haystack] at or after [from], or -1 if absent. */
    private fun indexOf(haystack: ByteArray, needle: ByteArray, from: Int): Int {
      if (needle.isEmpty()) return from
      var i = from
      while (i <= haystack.size - needle.size) {
        var j = 0
        while (j < needle.size && haystack[i + j] == needle[j]) {
          j++
        }
        if (j == needle.size) return i
        i++
      }
      return -1
    }
  }
}

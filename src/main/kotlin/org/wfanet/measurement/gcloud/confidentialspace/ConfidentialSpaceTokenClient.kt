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
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollDomainSocketChannel
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.unix.DomainSocketAddress
import io.netty.handler.codec.http.DefaultFullHttpRequest
import io.netty.handler.codec.http.FullHttpRequest
import io.netty.handler.codec.http.FullHttpResponse
import io.netty.handler.codec.http.HttpClientCodec
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpVersion
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

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
 * [DEFAULT_SOCKET_PATH]). The connection uses Netty's epoll domain-socket transport, and Netty's
 * HTTP codec frames the exchange, so `Transfer-Encoding: chunked` and `Content-Length` responses
 * are handled by the codec rather than by hand. One request/response is exchanged per call with
 * `Connection: close`.
 *
 * Domain sockets require the native epoll transport, so this client only runs on Linux. That is
 * where Confidential Space workloads run; [getToken] fails fast elsewhere.
 *
 * @param socketPath Filesystem path of the launcher token socket.
 * @param readTimeout Bound on how long a single token request may take.
 */
class ConfidentialSpaceTokenClient(
  private val socketPath: Path = Paths.get(DEFAULT_SOCKET_PATH),
  private val readTimeout: Duration = DEFAULT_READ_TIMEOUT,
) : AttestationTokenProvider {

  override fun getToken(request: AttestationTokenRequest): String {
    check(Epoll.isAvailable()) {
      "Netty epoll transport is unavailable, so the launcher token socket cannot be reached. " +
        "Confidential Space workloads run on Linux, where it is supported."
    }

    val timeoutMillis: Long = readTimeout.toMillis().coerceIn(1L, Int.MAX_VALUE.toLong())
    val responseFuture = CompletableFuture<LauncherResponse>()
    // One event loop per call: tokens are fetched rarely (on credential refresh), so a shared
    // group would outlive its usefulness and need a close() on this client's public API.
    val eventLoopGroup = EpollEventLoopGroup(1)
    try {
      val bootstrap =
        Bootstrap()
          .group(eventLoopGroup)
          .channel(EpollDomainSocketChannel::class.java)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis.toInt())
          .handler(
            object : ChannelInitializer<Channel>() {
              override fun initChannel(channel: Channel) {
                channel
                  .pipeline()
                  .addLast(
                    HttpClientCodec(),
                    HttpObjectAggregator(MAX_RESPONSE_SIZE_BYTES),
                    ResponseHandler(responseFuture),
                  )
              }
            }
          )

      val channel: Channel =
        try {
          bootstrap.connect(DomainSocketAddress(socketPath.toFile())).sync().channel()
        } catch (e: Exception) {
          throw IOException("Failed to connect to launcher token socket at $socketPath", e)
        }
      channel.writeAndFlush(buildHttpRequest(buildRequestBody(request)))

      val response: LauncherResponse =
        try {
          responseFuture.get(timeoutMillis, TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
          throw IOException("Timed out awaiting a response from the launcher token socket", e)
        } catch (e: ExecutionException) {
          throw IOException("Failed to read from the launcher token socket", e.cause ?: e)
        }

      val token = response.body.trim()
      if (response.statusCode !in 200..299) {
        throw IOException(
          "Launcher token request failed: ${response.statusCode} ${response.reasonPhrase}; " +
            "body=${redactIfJwt(token)}"
        )
      }
      check(token.isNotEmpty()) { "Launcher returned an empty attestation token" }
      return token
    } finally {
      // Zero quiet period: nothing else shares this group, so there is no work to drain.
      eventLoopGroup.shutdownGracefully(0, timeoutMillis, TimeUnit.MILLISECONDS)
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
          add(
            "aws_principal_tag_options",
            JsonObject().apply {
              add(
                "allowed_principal_tags",
                JsonObject().apply {
                  add(
                    "container_image_signatures",
                    JsonObject().apply {
                      add(
                        "key_ids",
                        JsonArray().apply {
                          request.containerImageSignatureKeyIds.forEach { add(it) }
                        },
                      )
                    },
                  )
                },
              )
            },
          )
        }
      }
      .toString()

  private fun buildHttpRequest(body: String): FullHttpRequest {
    val content = Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)
    val httpRequest =
      DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, TOKEN_PATH, content)
    httpRequest.headers().apply {
      set(HttpHeaderNames.HOST, "localhost")
      set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
      setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
      set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
    }
    return httpRequest
  }

  /** Status and body of the launcher's HTTP response, copied off the event loop. */
  private data class LauncherResponse(
    val statusCode: Int,
    val reasonPhrase: String,
    val body: String,
  )

  /**
   * Completes [future] with the aggregated response. The body is copied into a [String] here
   * because Netty releases the underlying buffer once this handler returns.
   */
  private class ResponseHandler(private val future: CompletableFuture<LauncherResponse>) :
    SimpleChannelInboundHandler<FullHttpResponse>() {

    override fun channelRead0(context: ChannelHandlerContext, message: FullHttpResponse) {
      // A malformed response (e.g. bad chunk framing) surfaces as a failed decoder result rather
      // than an exception, and would otherwise look like an empty body.
      val decoderResult = message.decoderResult()
      if (decoderResult.isFailure) {
        future.completeExceptionally(
          IOException(
            "Malformed HTTP response from the launcher token socket",
            decoderResult.cause(),
          )
        )
        context.close()
        return
      }
      future.complete(
        LauncherResponse(
          message.status().code(),
          message.status().reasonPhrase(),
          message.content().toString(StandardCharsets.UTF_8),
        )
      )
      context.close()
    }

    override fun channelInactive(context: ChannelHandlerContext) {
      // A no-op once the future is already complete, which is the normal close after a response.
      future.completeExceptionally(
        IOException("Launcher token socket closed before a complete response was received")
      )
      super.channelInactive(context)
    }

    override fun exceptionCaught(context: ChannelHandlerContext, cause: Throwable) {
      future.completeExceptionally(cause)
      context.close()
    }
  }

  companion object {
    /** Default Unix domain socket exposed by the Confidential Space launcher. */
    const val DEFAULT_SOCKET_PATH = "/run/container_launcher/teeserver.sock"
    /** Path of the launcher token endpoint. */
    const val TOKEN_PATH = "/v1/token"

    private val DEFAULT_READ_TIMEOUT: Duration = Duration.ofSeconds(30)
    /** Upper bound on the aggregated response; attestation tokens are a few KiB. */
    private const val MAX_RESPONSE_SIZE_BYTES = 1 shl 20

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

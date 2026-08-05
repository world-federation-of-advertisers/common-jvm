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

import com.google.common.truth.Truth.assertThat
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerDomainSocketChannel
import io.netty.channel.unix.DomainSocketAddress
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/**
 * Tests for [ConfidentialSpaceTokenClient] against a Netty Unix domain socket server that replies
 * with raw bytes, so the exact response framing the real launcher uses (chunked, Content-Length,
 * and unframed) can be exercised.
 */
@RunWith(JUnit4::class)
class ConfidentialSpaceTokenClientTest {
  private var serverGroup: EpollEventLoopGroup? = null
  private var serverChannel: Channel? = null
  private lateinit var socketPath: Path
  @Volatile private var capturedRequest: String = ""

  @Before
  fun assumeEpollAvailable() {
    // Domain sockets need the native epoll transport, which is Linux-only.
    assumeTrue(Epoll.isAvailable())
  }

  /** Binds a domain socket that replies with [response] verbatim, then closes. */
  private fun startServer(response: String) {
    // Bind under /tmp: the sun_path limit for a Unix socket is ~108 bytes, which Bazel's much
    // longer test tmpdir would exceed. The path must not exist yet for bind() to succeed.
    socketPath = Paths.get("/tmp", "cs-token-test-${System.nanoTime()}.sock")
    val group = EpollEventLoopGroup(1)
    serverGroup = group
    val received = StringBuilder()
    serverChannel =
      ServerBootstrap()
        .group(group)
        .channel(EpollServerDomainSocketChannel::class.java)
        .childHandler(
          object : ChannelInitializer<Channel>() {
            override fun initChannel(channel: Channel) {
              channel
                .pipeline()
                .addLast(
                  object : ChannelInboundHandlerAdapter() {
                    override fun channelRead(context: ChannelHandlerContext, message: Any) {
                      val buffer = message as ByteBuf
                      try {
                        received.append(buffer.toString(StandardCharsets.UTF_8))
                      } finally {
                        buffer.release()
                      }
                      // The request is complete once the headers and the JSON body have arrived.
                      if (received.contains("\r\n\r\n") && received.trimEnd().endsWith("}")) {
                        capturedRequest = received.toString()
                        context
                          .writeAndFlush(Unpooled.copiedBuffer(response, StandardCharsets.UTF_8))
                          .addListener(ChannelFutureListener.CLOSE)
                      }
                    }
                  }
                )
            }
          }
        )
        .bind(DomainSocketAddress(socketPath.toFile()))
        .sync()
        .channel()
  }

  @After
  fun stopServer() {
    serverChannel?.close()?.sync()
    serverGroup?.shutdownGracefully(0, 5, java.util.concurrent.TimeUnit.SECONDS)
    if (this::socketPath.isInitialized) {
      Files.deleteIfExists(socketPath)
    }
  }

  private fun clientForServer() =
    ConfidentialSpaceTokenClient(socketPath = socketPath, requestTimeout = Duration.ofSeconds(10))

  private fun awsPrincipalTagsRequest() =
    AttestationTokenRequest(
      audience = "https://example.com",
      tokenType = ConfidentialSpaceTokenType.AWS_PRINCIPAL_TAGS,
    )

  @Test
  fun `getToken returns the token body from an unframed 200 response`() {
    val token = "header.payload.signature"
    startServer("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n$token")

    val result = clientForServer().getToken(awsPrincipalTagsRequest())

    assertThat(result).isEqualTo(token)
    assertThat(capturedRequest).contains("POST /v1/token HTTP/1.1")
    assertThat(capturedRequest).contains("\"token_type\":\"AWS_PRINCIPALTAGS\"")
    assertThat(capturedRequest).contains("\"audience\":\"https://example.com\"")
  }

  @Test
  fun `getToken de-chunks a Transfer-Encoding chunked response`() {
    // The launcher sets no Content-Length, so real (large) tokens arrive chunked. Split the token
    // across two chunks (sizes 7 and 0x11) to exercise reassembly.
    val response =
      "HTTP/1.1 200 OK\r\n" +
        "Content-Type: text/plain\r\n" +
        "Transfer-Encoding: chunked\r\n" +
        "Connection: close\r\n" +
        "\r\n" +
        "7\r\nheader.\r\n" +
        "11\r\npayload.signature\r\n" +
        "0\r\n\r\n"
    startServer(response)

    val result = clientForServer().getToken(awsPrincipalTagsRequest())

    assertThat(result).isEqualTo("header.payload.signature")
  }

  @Test
  fun `getToken honors Content-Length and ignores trailing bytes`() {
    val token = "header.payload.signature"
    startServer(
      "HTTP/1.1 200 OK\r\nContent-Length: ${token.length}\r\nConnection: close\r\n\r\n${token}EXTRA"
    )

    val result = clientForServer().getToken(awsPrincipalTagsRequest())

    assertThat(result).isEqualTo(token)
  }

  @Test
  fun `getToken accepts a 2xx status other than 200`() {
    val token = "header.payload.signature"
    startServer(
      "HTTP/1.1 202 Accepted\r\nContent-Length: ${token.length}\r\nConnection: close\r\n\r\n$token"
    )

    val result = clientForServer().getToken(awsPrincipalTagsRequest())

    assertThat(result).isEqualTo(token)
  }

  @Test
  fun `getToken throws on a non-2xx response`() {
    startServer(
      "HTTP/1.1 400 Bad Request\r\nContent-Length: 16\r\nConnection: close\r\n\r\ninvalid audience"
    )

    val exception =
      assertFailsWith<IOException> { clientForServer().getToken(awsPrincipalTagsRequest()) }

    assertThat(exception).hasMessageThat().contains("400")
    assertThat(exception).hasMessageThat().contains("invalid audience")
  }

  @Test
  fun `getToken redacts a JWT-looking body from the error message`() {
    val jwt = "header.payload.signature"
    startServer(
      "HTTP/1.1 401 Unauthorized\r\nContent-Length: ${jwt.length}\r\nConnection: close\r\n\r\n$jwt"
    )

    val exception =
      assertFailsWith<IOException> { clientForServer().getToken(awsPrincipalTagsRequest()) }

    assertThat(exception).hasMessageThat().contains("[redacted possible JWT]")
    assertThat(exception).hasMessageThat().doesNotContain(jwt)
  }

  @Test
  fun `getToken fails on a truncated chunk`() {
    // Chunk header claims 0x20 (32) bytes but far fewer follow before the socket closes.
    startServer(
      "HTTP/1.1 200 OK\r\n" +
        "Transfer-Encoding: chunked\r\n" +
        "Connection: close\r\n" +
        "\r\n" +
        "20\r\nonly-a-few-bytes\r\n"
    )

    assertFailsWith<IOException> { clientForServer().getToken(awsPrincipalTagsRequest()) }
  }

  @Test
  fun `getToken fails on an invalid chunk size`() {
    startServer(
      "HTTP/1.1 200 OK\r\n" +
        "Transfer-Encoding: chunked\r\n" +
        "Connection: close\r\n" +
        "\r\n" +
        "zz\r\nsome-bytes\r\n0\r\n\r\n"
    )

    assertFailsWith<IOException> { clientForServer().getToken(awsPrincipalTagsRequest()) }
  }

  @Test
  fun `getToken sends aws_principal_tag_options for an AWS_PRINCIPALTAGS request`() {
    startServer("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nheader.payload.signature")

    clientForServer().getToken(awsPrincipalTagsRequest())

    assertThat(capturedRequest).contains("aws_principal_tag_options")
    assertThat(capturedRequest).contains("allowed_principal_tags")
    assertThat(capturedRequest).contains("container_image_signatures")
  }

  @Test
  fun `getToken omits aws_principal_tag_options for non-AWS token types`() {
    startServer("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nheader.payload.signature")

    clientForServer()
      .getToken(
        AttestationTokenRequest(
          audience = "https://example.com",
          tokenType = ConfidentialSpaceTokenType.OIDC,
        )
      )

    assertThat(capturedRequest).contains("\"token_type\":\"OIDC\"")
    assertThat(capturedRequest).doesNotContain("aws_principal_tag_options")
  }

  @Test
  fun `getToken throws a clear error when the launcher sends no response`() {
    startServer("")

    val exception =
      assertFailsWith<IOException> { clientForServer().getToken(awsPrincipalTagsRequest()) }

    assertThat(exception)
      .hasCauseThat()
      .hasMessageThat()
      .contains("closed before a complete response")
  }

  @Test
  fun `getToken includes requested container image signature key ids`() {
    startServer("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nheader.payload.signature")

    clientForServer()
      .getToken(
        AttestationTokenRequest(
          audience = "https://example.com",
          tokenType = ConfidentialSpaceTokenType.AWS_PRINCIPAL_TAGS,
          containerImageSignatureKeyIds = listOf("keyA", "keyB"),
        )
      )

    assertThat(capturedRequest).contains("container_image_signatures")
    assertThat(capturedRequest).contains("key_ids")
    assertThat(capturedRequest).contains("keyA")
    assertThat(capturedRequest).contains("keyB")
  }

  @Test
  fun `parseContainerImageSignatureKeyIds extracts key ids from the signatures claim`() {
    val payload =
      """
      {"submods":{"container":{"image_signatures":[
        {"key_id":"keyA","signature_algorithm":"ECDSA_P256_SHA256"},
        {"key_id":"keyB","signature_algorithm":"ECDSA_P256_SHA256"},
        {"key_id":"keyA","signature_algorithm":"ECDSA_P256_SHA256"}
      ]}}}
      """
        .trimIndent()
    val encodedPayload =
      java.util.Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(payload.toByteArray(StandardCharsets.UTF_8))

    val keyIds =
      ConfidentialSpaceTokenClient.parseContainerImageSignatureKeyIds(
        "header.$encodedPayload.signature"
      )

    assertThat(keyIds).containsExactly("keyA", "keyB").inOrder()
  }
}

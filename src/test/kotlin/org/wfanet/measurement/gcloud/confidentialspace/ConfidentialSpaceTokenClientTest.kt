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
import java.io.IOException
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.Base64
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/**
 * Tests for [ConfidentialSpaceTokenClient] against a Unix domain socket server that replies with
 * raw bytes, so the exact response framing the real launcher uses (chunked, Content-Length, and
 * unframed) can be exercised.
 */
@RunWith(JUnit4::class)
class ConfidentialSpaceTokenClientTest {
  private var serverChannel: ServerSocketChannel? = null
  private var serverThread: Thread? = null
  private lateinit var socketPath: Path
  @Volatile private var capturedRequest: String = ""

  /** Binds a domain socket that replies with [responses] in order, one per connection. */
  private fun startServer(vararg responses: String) {
    // Bind under /tmp: the sun_path limit for a Unix socket is ~108 bytes, which Bazel's much
    // longer test tmpdir would exceed. The path must not exist yet for bind() to succeed.
    socketPath = Paths.get("/tmp", "cs-token-test-${System.nanoTime()}.sock")
    val channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    channel.bind(UnixDomainSocketAddress.of(socketPath))
    serverChannel = channel

    serverThread =
      thread(start = true, isDaemon = true) {
        var index = 0
        try {
          while (channel.isOpen) {
            channel.accept().use { connection -> serve(connection, responses[index]) }
            if (index < responses.size - 1) {
              index++
            }
          }
        } catch (e: IOException) {
          // Expected once the channel is closed in tearDown.
        }
      }
  }

  /** Reads one request off [connection], records it, then writes [response]. */
  private fun serve(connection: SocketChannel, response: String) {
    val received = StringBuilder()
    val buffer = ByteBuffer.allocate(8192)
    while (true) {
      buffer.clear()
      if (connection.read(buffer) <= 0) break
      buffer.flip()
      received.append(StandardCharsets.UTF_8.decode(buffer))
      // The request is complete once the headers and the JSON body have arrived.
      if (received.contains("\r\n\r\n") && received.trimEnd().endsWith("}")) break
    }
    capturedRequest = received.toString()
    val out = ByteBuffer.wrap(response.toByteArray(StandardCharsets.UTF_8))
    while (out.hasRemaining()) {
      connection.write(out)
    }
  }

  @After
  fun tearDown() {
    serverChannel?.close()
    serverThread?.join(TimeUnit.SECONDS.toMillis(5))
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
  fun `getToken returns the token body from a Content-Length response`() {
    val token = "header.payload.signature"
    startServer(
      "HTTP/1.1 200 OK\r\nContent-Length: ${token.length}\r\nConnection: close\r\n\r\n$token"
    )

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
    startServer(
      "HTTP/1.1 200 OK\r\n" +
        "Content-Type: text/plain\r\n" +
        "Transfer-Encoding: chunked\r\n" +
        "Connection: close\r\n" +
        "\r\n" +
        "7\r\nheader.\r\n" +
        "11\r\npayload.signature\r\n" +
        "0\r\n\r\n"
    )

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
  fun `getToken fails on an empty response`() {
    startServer("")

    assertFailsWith<IOException> { clientForServer().getToken(awsPrincipalTagsRequest()) }
  }

  @Test
  fun `getToken sends aws_principal_tag_options for an AWS_PRINCIPALTAGS request`() {
    startServer("HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\nheader.payload.signature")

    clientForServer().getToken(awsPrincipalTagsRequest())

    assertThat(capturedRequest).contains("aws_principal_tag_options")
    assertThat(capturedRequest).contains("allowed_principal_tags")
    assertThat(capturedRequest).contains("container_image_signatures")
  }

  @Test
  fun `getToken omits aws_principal_tag_options for non-AWS token types`() {
    startServer("HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\nheader.payload.signature")

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
  fun `getToken includes requested container image signature key ids`() {
    startServer("HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\nheader.payload.signature")

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
  fun `getToken reuses a pooled connection for a later request`() {
    // A second POST exercises OkHttp's pooled-connection health check, which sets a 1ms socket
    // timeout and reads. That blocks forever unless the socket honors SO_TIMEOUT.
    val body = "header.payload.signature"
    val response = "HTTP/1.1 200 OK\r\nContent-Length: ${body.length}\r\n\r\n$body"
    startServer(response, response)
    val client = clientForServer()

    assertThat(client.getToken(awsPrincipalTagsRequest())).isEqualTo(body)
    assertThat(client.getToken(awsPrincipalTagsRequest())).isEqualTo(body)
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
      Base64.getUrlEncoder().withoutPadding().encodeToString(payload.toByteArray())

    val keyIds =
      ConfidentialSpaceTokenClient.parseContainerImageSignatureKeyIds(
        "header.$encodedPayload.signature"
      )

    assertThat(keyIds).containsExactly("keyA", "keyB").inOrder()
  }
}

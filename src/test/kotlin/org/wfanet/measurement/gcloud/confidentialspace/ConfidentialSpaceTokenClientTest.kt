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
import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import kotlin.concurrent.thread
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.newsclub.net.unix.AFUNIXServerSocket
import org.newsclub.net.unix.AFUNIXSocket
import org.newsclub.net.unix.AFUNIXSocketAddress

/**
 * Tests for [ConfidentialSpaceTokenClient].
 *
 * A real in-process Unix domain socket server (junixsocket, Linux abstract namespace to avoid
 * socket-path length limits) stands in for the Confidential Space launcher, exercising the HTTP
 * response framing the real launcher uses (chunked and Content-Length).
 */
@RunWith(JUnit4::class)
class ConfidentialSpaceTokenClientTest {
  private lateinit var serverAddress: AFUNIXSocketAddress
  private lateinit var serverSocket: AFUNIXServerSocket
  private lateinit var serverThread: Thread
  @Volatile private var capturedRequest: String = ""

  private fun startServer(response: String) {
    serverAddress = AFUNIXSocketAddress.inAbstractNamespace("cs-token-test-" + System.nanoTime())
    serverSocket = AFUNIXServerSocket.bindOn(serverAddress)
    serverThread =
      thread(start = true) {
        serverSocket.accept().use { conn ->
          conn.soTimeout = 2_000
          val buffer = ByteArray(8192)
          val received = StringBuilder()
          try {
            while (true) {
              val read = conn.getInputStream().read(buffer)
              if (read <= 0) break
              received.append(String(buffer, 0, read, StandardCharsets.UTF_8))
              if (received.contains("\r\n\r\n") && received.trimEnd().endsWith("}")) break
            }
          } catch (e: SocketTimeoutException) {
            // Done reading the (small) request.
          }
          capturedRequest = received.toString()
          conn.getOutputStream().apply {
            write(response.toByteArray(StandardCharsets.UTF_8))
            flush()
          }
        }
      }
  }

  @After
  fun tearDown() {
    if (this::serverSocket.isInitialized) serverSocket.close()
    if (this::serverThread.isInitialized) serverThread.join(5_000)
  }

  private fun clientForServer() =
    ConfidentialSpaceTokenClient(
      socketPath = Paths.get("unused-in-test"),
      socketFactory = { AFUNIXSocket.connectTo(serverAddress) },
    )

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
    startServer("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\ninvalid audience")

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
  fun `getToken throws a clear error on an empty response`() {
    startServer("")

    val exception =
      assertFailsWith<IllegalArgumentException> {
        clientForServer().getToken(awsPrincipalTagsRequest())
      }
    assertThat(exception).hasMessageThat().contains("Empty response")
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
}

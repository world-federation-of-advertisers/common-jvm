// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.kms

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList

/** Serves synthetic STS and KMS responses to the real AWS SDK. */
internal class FederationTestServer : AutoCloseable {
  data class Response(val status: Int, val body: String, val headers: Map<String, String>)

  val stsRequests = CopyOnWriteArrayList<String>()
  val kmsRequests = CopyOnWriteArrayList<String>()
  val kmsAuthorizationHeaders = CopyOnWriteArrayList<String>()
  val stsResponses = ConcurrentLinkedQueue<Response>()
  private val originalProperties =
    listOf("aws.endpointUrl", "aws.maxAttempts").associateWith { System.getProperty(it) }
  private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)

  init {
    server.createContext("/") { exchange ->
      val body = exchange.requestBody.bufferedReader().use { it.readText() }
      val response =
        if (exchange.requestHeaders.getFirst("X-Amz-Target") != null) {
          kmsRequests.add(body)
          kmsAuthorizationHeaders.add(exchange.requestHeaders.getFirst("Authorization"))
          Response(
            200,
            """{"CiphertextBlob":"Y2lwaGVydGV4dA=="}""",
            mapOf("Content-Type" to "application/x-amz-json-1.1"),
          )
        } else {
          stsRequests.add(body)
          stsResponses.poll() ?: Response(200, STS_SUCCESS, mapOf("Content-Type" to "text/xml"))
        }
      for ((name, value) in response.headers) exchange.responseHeaders.set(name, value)
      val bytes = response.body.toByteArray()
      exchange.sendResponseHeaders(response.status, bytes.size.toLong())
      exchange.responseBody.use { it.write(bytes) }
      exchange.close()
    }
    server.start()
    System.setProperty("aws.endpointUrl", "http://127.0.0.1:" + server.address.port)
    // Disable SDK retries so these tests observe the factory's retry policy.
    System.setProperty("aws.maxAttempts", "1")
  }

  override fun close() {
    server.stop(0)
    for ((name, originalValue) in originalProperties) {
      if (originalValue == null) System.clearProperty(name)
      else System.setProperty(name, originalValue)
    }
  }

  companion object {
    fun failure(status: Int, code: String, retryAfter: String?): Response =
      Response(
        status,
        """<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
          <Error><Type>Sender</Type><Code>$code</Code><Message>test failure</Message></Error>
          <RequestId>test-request</RequestId></ErrorResponse>""",
        mapOf("Content-Type" to "text/xml") +
          if (retryAfter == null) emptyMap() else mapOf("Retry-After" to retryAfter),
      )

    private val STS_SUCCESS =
      """<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
      <AssumeRoleWithWebIdentityResult><Credentials>
      <AccessKeyId>test-access-key</AccessKeyId><SecretAccessKey>test-secret-key</SecretAccessKey>
      <SessionToken>test-session-token</SessionToken><Expiration>2100-01-01T00:00:00Z</Expiration>
      </Credentials></AssumeRoleWithWebIdentityResult>
      <ResponseMetadata><RequestId>test-request</RequestId></ResponseMetadata>
      </AssumeRoleWithWebIdentityResponse>"""
  }
}

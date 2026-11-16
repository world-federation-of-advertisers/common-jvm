// Copyright 2025 The Cross-Media Measurement Authors
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

import com.google.api.client.testing.http.MockLowLevelHttpResponse
import com.google.common.truth.Truth.assertThat
import java.security.GeneralSecurityException
import java.time.Duration
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.test.scheduler.VirtualTimeScheduler

/** Exercises the factory with synthetic Google ID-token responses and a local AWS server. */
@RunWith(JUnit4::class)
class GCloudToAwsKmsClientFactoryTest {
  private lateinit var virtualTime: VirtualTimeScheduler
  private lateinit var executor: ExecutorService

  @Before
  fun setUp() {
    virtualTime = VirtualTimeScheduler.create()
    executor = Executors.newSingleThreadExecutor()
    GoogleIdTokenTestTransportFactory.requests.set(0)
    GoogleIdTokenTestTransportFactory.response = { url, _ ->
      check(url.endsWith(":generateIdToken"))
      MockLowLevelHttpResponse()
        .setContentType("application/json")
        .setContent("""{"token":"$ID_TOKEN"}""")
    }
  }

  @After
  fun tearDown() {
    if (!virtualTime.isDisposed) virtualTime.advanceTimeBy(Duration.ofMinutes(3))
    executor.shutdownNow()
    check(executor.awaitTermination(10, TimeUnit.SECONDS))
    VirtualTimeScheduler.reset()
  }

  @Test
  fun `getKmsClient with invalid config fails on first use`() {
    val invalidConfig =
      config().copy(serviceAccountImpersonationUrl = "https://example.com/invalid")
    val aead = GCloudToAwsKmsClientFactory().getKmsClient(invalidConfig).getAead(KEY_URI)
    assertFailsWith<GeneralSecurityException> { aead.encrypt(ByteArray(0), null) }
    assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(0)
  }

  @Test
  fun `encrypt obtains credentials through the Google and AWS libraries`() {
    FederationTestServer().use { server ->
      assertThat(encrypt().get(10, TimeUnit.SECONDS)).isEqualTo("ciphertext".toByteArray())
      assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
      assertThat(server.stsRequests).hasSize(1)
      assertThat(server.kmsRequests).hasSize(1)
    }
  }

  @Test
  fun `encrypt lets the SDK retry STS with the Google ID token already obtained`() {
    FederationTestServer().use { server ->
      repeat(FederationTestServer.SDK_MAX_ATTEMPTS - 1) {
        server.stsResponses.add(FederationTestServer.failure(503, "ServiceUnavailable", null))
      }

      assertThat(encrypt().get(10, TimeUnit.SECONDS)).isEqualTo("ciphertext".toByteArray())
      assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
      assertThat(server.stsRequests).hasSize(FederationTestServer.SDK_MAX_ATTEMPTS)
      assertThat(server.stsRequests.toSet()).hasSize(1)
      assertThat(server.stsRequests[0]).contains("WebIdentityToken=$ID_TOKEN")
    }
  }

  @Test
  fun `encrypt stops when the SDK STS attempt limit is exhausted`() {
    FederationTestServer().use { server ->
      repeat(FederationTestServer.SDK_MAX_ATTEMPTS) {
        server.stsResponses.add(FederationTestServer.failure(503, "ServiceUnavailable", null))
      }
      // The server would succeed on another request, which must not be sent.
      assertSecurityFailure(encrypt())

      assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
      assertThat(server.stsRequests).hasSize(FederationTestServer.SDK_MAX_ATTEMPTS)
      assertThat(server.stsRequests.toSet()).hasSize(1)
      assertThat(server.stsRequests[0]).contains("WebIdentityToken=$ID_TOKEN")
      assertThat(server.kmsRequests).isEmpty()
    }
  }

  @Test
  fun `encrypt leaves Google IAM permission failures to the library`() {
    FederationTestServer().use { server ->
      GoogleIdTokenTestTransportFactory.response = { _, _ ->
        MockLowLevelHttpResponse()
          .setStatusCode(403)
          .setContentType("application/json")
          .setContent("""{"error":{"message":"permission denied"}}""")
      }

      assertSecurityFailure(encrypt())
      assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
      assertThat(server.stsRequests).isEmpty()
    }
  }

  @Test
  fun `encrypt documents the Google IAM 503 retry gap`() {
    FederationTestServer().use { server ->
      GoogleIdTokenTestTransportFactory.response = { _, _ ->
        MockLowLevelHttpResponse()
          .setStatusCode(503)
          .setContentType("application/json")
          .setContent("""{"error":{"message":"temporarily unavailable"}}""")
      }
      assertSecurityFailure(encrypt())
      virtualTime.advanceTimeBy(Duration.ofMinutes(3))
      assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
      assertThat(server.stsRequests).isEmpty()
    }
  }

  @Test
  fun `encrypt bounds an unfinished Google ID token request to the refresh deadline`() {
    // Advance the timeout clock without running blocking Google HTTP work on that clock.
    val schedulerSnapshot =
      Schedulers.setFactoryWithSnapshot(
        object : Schedulers.Factory {
          override fun newParallel(parallelism: Int, threadFactory: ThreadFactory): Scheduler =
            virtualTime
        }
      )
    try {
      FederationTestServer().use { server ->
        val firstRequest = CountDownLatch(1)
        val releaseRequest = CountDownLatch(1)
        val success = GoogleIdTokenTestTransportFactory.response
        GoogleIdTokenTestTransportFactory.response = { url, attempt ->
          firstRequest.countDown()
          check(releaseRequest.await(10, TimeUnit.SECONDS)) {
            "Test did not release Google request"
          }
          success(url, attempt)
        }

        try {
          val encryption = encrypt()
          check(firstRequest.await(10, TimeUnit.SECONDS))
          awaitScheduledTasks(1)

          virtualTime.advanceTimeBy(Duration.ofMinutes(2))

          assertSecurityFailure(encryption)
          assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
          assertThat(server.stsRequests).isEmpty()
        } finally {
          releaseRequest.countDown()
        }
      }
    } finally {
      Schedulers.resetFrom(schedulerSnapshot)
    }
  }

  @Test
  fun `one KMS client supports key ARNs in different regions`() {
    FederationTestServer().use { server ->
      val client = GCloudToAwsKmsClientFactory().getKmsClient(config())
      for (region in listOf("us-east-1", "us-west-2")) {
        val aead = client.getAead("aws-kms://arn:aws:kms:$region:123456789012:key/test-key")
        assertThat(aead.encrypt(ByteArray(0), null)).isEqualTo("ciphertext".toByteArray())
      }
      assertThat(GoogleIdTokenTestTransportFactory.requests.get()).isEqualTo(1)
      assertThat(server.stsRequests).hasSize(1)
      assertThat(server.kmsAuthorizationHeaders).hasSize(2)
      assertThat(server.kmsAuthorizationHeaders[0]).contains("/us-east-1/kms/aws4_request")
      assertThat(server.kmsAuthorizationHeaders[1]).contains("/us-west-2/kms/aws4_request")
    }
  }

  private fun config(): GCloudToAwsWifCredentials =
    GCloudToAwsWifCredentials(
      gcloudAudience =
        "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
      subjectTokenType = "urn:ietf:params:oauth:token-type:jwt",
      tokenUrl = "https://sts.googleapis.com/v1/token",
      credentialSourceFilePath = "/unused-test-token",
      serviceAccountImpersonationUrl =
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@example.com:generateAccessToken",
      roleArn = "arn:aws:iam::123456789012:role/test-role",
      roleSessionName = "test-session",
      region = "us-east-1",
      awsAudience = "https://example.com/oidc",
    )

  private fun encrypt(): Future<ByteArray> =
    executor.submit<ByteArray> {
      GCloudToAwsKmsClientFactory()
        .getKmsClient(config())
        .getAead(KEY_URI)
        .encrypt(ByteArray(0), null)
    }

  private fun assertSecurityFailure(encryption: Future<ByteArray>) {
    val failure = assertFailsWith<ExecutionException> { encryption.get(10, TimeUnit.SECONDS) }
    assertThat(failure.cause).isInstanceOf(GeneralSecurityException::class.java)
  }

  private fun awaitScheduledTasks(count: Long) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    virtualTime.advanceTimeBy(Duration.ZERO)
    while (virtualTime.scheduledTaskCount < count) {
      virtualTime.advanceTimeBy(Duration.ZERO)
      check(System.nanoTime() < deadline) { "Credential operation did not schedule its deadline" }
      Thread.sleep(1)
    }
  }

  companion object {
    private const val KEY_URI = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key"
    private val ID_TOKEN =
      listOf(
          """{"alg":"RS256"}""",
          """{"aud":"https://example.com/oidc","exp":4102444800,"iat":1700000000,"sub":"test"}""",
          "signature",
        )
        .joinToString(".") {
          Base64.getUrlEncoder().withoutPadding().encodeToString(it.toByteArray())
        }
  }
}

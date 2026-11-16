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

package org.wfanet.measurement.gcloud.kms

import com.google.api.client.http.HttpHeaders
import com.google.api.client.http.HttpResponseException
import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import java.io.IOException
import java.security.GeneralSecurityException
import java.time.Duration
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.ConfidentialSpaceToAwsWifCredentials
import org.wfanet.measurement.gcloud.confidentialspace.AttestationTokenProvider
import org.wfanet.measurement.gcloud.confidentialspace.AttestationTokenRequest
import org.wfanet.measurement.gcloud.confidentialspace.ConfidentialSpaceTokenType
import reactor.test.scheduler.VirtualTimeScheduler

/**
 * Exercises credential retries through the real Tink/AWS client, before any AWS request is sent.
 */
@RunWith(JUnit4::class)
class ConfidentialSpaceToAwsKmsClientFactoryTest {
  private lateinit var virtualTime: VirtualTimeScheduler
  private lateinit var executor: ExecutorService
  private val requests = AtomicInteger()
  private val firstRequest = CountDownLatch(1)

  @Before
  fun setUp() {
    virtualTime = VirtualTimeScheduler.getOrSet()
    executor = Executors.newSingleThreadExecutor()
  }

  @After
  fun tearDown() {
    // Finish a blocked SDK call even if an assertion failed before advancing the refresh budget.
    virtualTime.advanceTimeBy(Duration.ofMinutes(3))
    executor.shutdownNow()
    check(executor.awaitTermination(10, TimeUnit.SECONDS))
    VirtualTimeScheduler.reset()
  }

  @Test
  fun `encrypt recovers from STS failure without fetching the token again`() {
    FederationTestServer().use { server ->
      server.stsResponses.add(FederationTestServer.failure(503, "ServiceUnavailable", null))
      val encryption =
        encrypt(aead { CompletableFuture.completedFuture("header.payload.signature") })
      awaitScheduledTasks(3)
      virtualTime.advanceTimeBy(Duration.ofSeconds(2))
      assertThat(encryption.get(10, TimeUnit.SECONDS)).isEqualTo("ciphertext".toByteArray())
      assertThat(server.stsRequests).hasSize(2)
      assertThat(server.stsRequests[0]).isEqualTo(server.stsRequests[1])
      assertThat(server.kmsRequests).hasSize(1)
      assertThat(requests.get()).isEqualTo(1)
    }
  }

  @Test
  fun `encrypt respects the STS Retry-After header`() {
    FederationTestServer().use { server ->
      server.stsResponses.add(FederationTestServer.failure(503, "ServiceUnavailable", "75"))
      val encryption =
        encrypt(aead { CompletableFuture.completedFuture("header.payload.signature") })
      awaitScheduledTasks(3)
      virtualTime.advanceTimeBy(Duration.ofSeconds(74))
      assertThat(server.stsRequests).hasSize(1)
      assertThat(encryption.isDone).isFalse()
      virtualTime.advanceTimeBy(Duration.ofSeconds(2))
      assertThat(encryption.get(10, TimeUnit.SECONDS)).isEqualTo("ciphertext".toByteArray())
      assertThat(server.stsRequests).hasSize(2)
      assertThat(requests.get()).isEqualTo(1)
    }
  }

  @Test
  fun `encrypt does not retry a permanent STS failure`() {
    FederationTestServer().use { server ->
      server.stsResponses.add(FederationTestServer.failure(403, "AccessDenied", null))
      assertSecurityFailure(
        encrypt(aead { CompletableFuture.completedFuture("header.payload.signature") })
      )
      virtualTime.advanceTimeBy(Duration.ofMinutes(3))
      assertThat(server.stsRequests).hasSize(1)
      assertThat(server.kmsRequests).isEmpty()
      assertThat(requests.get()).isEqualTo(1)
    }
  }

  @Test
  fun `encrypt discovers signing keys once before retrying STS`() {
    FederationTestServer().use { server ->
      server.stsResponses.add(FederationTestServer.failure(503, "ServiceUnavailable", null))
      val tokenRequests = java.util.concurrent.CopyOnWriteArrayList<AttestationTokenRequest>()
      val oidcPayload =
        """{"submods":{"container":{"image_signatures":[{"key_id":"signing-key"}]}}}"""
      val oidcToken =
        "header." +
          Base64.getUrlEncoder().withoutPadding().encodeToString(oidcPayload.toByteArray()) +
          ".signature"
      val provider = AttestationTokenProvider { request ->
        tokenRequests.add(request)
        CompletableFuture.completedFuture(
          if (request.tokenType == ConfidentialSpaceTokenType.OIDC) oidcToken else "aws-token"
        )
      }
      val encryption =
        encrypt(
          ConfidentialSpaceToAwsKmsClientFactory(tokenProvider = provider)
            .getKmsClient(config(emptyList()))
            .getAead(KEY_URI)
        )
      awaitScheduledTasks(3)
      virtualTime.advanceTimeBy(Duration.ofSeconds(2))
      assertThat(encryption.get(10, TimeUnit.SECONDS)).isEqualTo("ciphertext".toByteArray())
      assertThat(tokenRequests.map { it.tokenType })
        .containsExactly(
          ConfidentialSpaceTokenType.OIDC,
          ConfidentialSpaceTokenType.AWS_PRINCIPAL_TAGS,
        )
        .inOrder()
      assertThat(tokenRequests[1].containerImageSignatureKeyIds).containsExactly("signing-key")
      assertThat(server.stsRequests).hasSize(2)
    }
  }

  @Test
  fun `encrypt leaves signature-discovery failures to the token provider`() {
    val calls = AtomicInteger()
    val provider = AttestationTokenProvider {
      calls.incrementAndGet()
      httpFailure(503)
    }
    val encryption =
      encrypt(
        ConfidentialSpaceToAwsKmsClientFactory(tokenProvider = provider)
          .getKmsClient(config(emptyList()))
          .getAead(KEY_URI)
      )
    assertSecurityFailure(encryption)
    virtualTime.advanceTimeBy(Duration.ofMinutes(3))
    assertThat(calls.get()).isEqualTo(1)
  }

  @Test
  fun `encrypt leaves token-provider HTTP failures to the provider`() {
    for (status in listOf(429, 503, 403, 501)) {
      val before = requests.get()
      val clientAead = aead { httpFailure(status) }
      assertSecurityFailure(encrypt(clientAead))
      virtualTime.advanceTimeBy(Duration.ofMinutes(3))
      assertThat(requests.get()).isEqualTo(before + 1)
      // A failed refresh does not poison the reusable client.
      assertSecurityFailure(encrypt(clientAead))
      assertThat(requests.get()).isEqualTo(before + 2)
    }
  }

  @Test
  fun `encrypt cancels an unfinished token request at the refresh deadline`() {
    val token = CompletableFuture<String>()
    val encryption = encrypt(aead { token })
    check(firstRequest.await(10, TimeUnit.SECONDS))
    awaitScheduledTasks(1)

    virtualTime.advanceTimeBy(Duration.ofMinutes(2))

    assertSecurityFailure(encryption)
    assertThat(token.isCancelled).isTrue()
    assertThat(requests.get()).isEqualTo(1)
  }

  @Test
  fun `encrypt does not retry an unclassified token failure`() {
    val encryption =
      encrypt(aead { CompletableFuture.failedFuture(IOException("token unavailable")) })
    assertSecurityFailure(encryption)
    virtualTime.advanceTimeBy(Duration.ofMinutes(3))
    assertThat(requests.get()).isEqualTo(1)
  }

  @Test
  fun `encrypt does not retry permission or unsupported service responses`() {
    for (status in listOf(403, 501)) {
      val before = requests.get()
      assertSecurityFailure(encrypt(aead { httpFailure(status) }))
      assertThat(requests.get()).isEqualTo(before + 1)
    }
  }

  private fun aead(token: (Int) -> CompletableFuture<String>): Aead {
    val provider = AttestationTokenProvider {
      val attempt = requests.incrementAndGet()
      firstRequest.countDown()
      token(attempt)
    }
    return ConfidentialSpaceToAwsKmsClientFactory(tokenProvider = provider)
      .getKmsClient(config(listOf("signing-key")))
      .getAead(KEY_URI)
  }

  private fun config(signatureKeyIds: List<String>): ConfidentialSpaceToAwsWifCredentials =
    ConfidentialSpaceToAwsWifCredentials(
      roleArn = "arn:aws:iam::123456789012:role/test-role",
      roleSessionName = "test-session",
      region = "us-east-1",
      audience = "https://example.com",
      containerImageSignatureKeyIds = signatureKeyIds,
    )

  private fun httpFailure(status: Int): CompletableFuture<String> =
    CompletableFuture.failedFuture(
      HttpResponseException.Builder(status, null, HttpHeaders()).build()
    )

  private fun encrypt(aead: Aead): Future<ByteArray> =
    executor.submit<ByteArray> { aead.encrypt(ByteArray(0), null) }

  private fun assertSecurityFailure(encryption: Future<ByteArray>) {
    val failure = assertFailsWith<ExecutionException> { encryption.get(10, TimeUnit.SECONDS) }
    assertThat(failure.cause).isInstanceOf(GeneralSecurityException::class.java)
  }

  private fun awaitScheduledTasks(count: Long) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    virtualTime.advanceTimeBy(Duration.ZERO)
    while (virtualTime.scheduledTaskCount < count) {
      virtualTime.advanceTimeBy(Duration.ZERO)
      check(System.nanoTime() < deadline) { "Credential operation did not schedule its timer" }
      Thread.sleep(1)
    }
  }

  companion object {
    private const val KEY_URI = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key"
  }
}

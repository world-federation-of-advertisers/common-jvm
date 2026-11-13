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
import java.io.IOException
import java.security.GeneralSecurityException
import java.util.concurrent.CompletableFuture
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.ConfidentialSpaceToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsKeyAccessException
import org.wfanet.measurement.gcloud.confidentialspace.AttestationTokenProvider

/**
 * Tests for [ConfidentialSpaceToAwsKmsClientFactory].
 *
 * Full integration testing requires a Confidential Space environment with a configured AWS trust
 * relationship. These tests verify retry information and failure handling through real AWS
 * credential resolution. The token-fetch path is covered by
 * [org.wfanet.measurement.gcloud.confidentialspace.ConfidentialSpaceTokenClientTest].
 */
@RunWith(JUnit4::class)
class ConfidentialSpaceToAwsKmsClientFactoryTest {

  @Test
  fun `getKmsClient with invalid config fails on first use`() {
    val factory = ConfidentialSpaceToAwsKmsClientFactory()
    val config =
      ConfidentialSpaceToAwsWifCredentials(
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        audience = "https://example.com",
      )
    val kmsClient = factory.getKmsClient(config)
    val aead = kmsClient.getAead("aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key")
    assertFailsWith<GeneralSecurityException> { aead.encrypt(ByteArray(0), null) }
  }

  @Test
  fun `encrypt retries attestation retrieval using the same client after a temporary failure`() {
    val original = HttpResponseException.Builder(429, "Too Many Requests", HttpHeaders()).build()
    var tokenRequests = 0
    val provider = AttestationTokenProvider {
      tokenRequests++
      CompletableFuture.failedFuture(original)
    }
    val factory = ConfidentialSpaceToAwsKmsClientFactory(tokenProvider = provider)
    val config =
      ConfidentialSpaceToAwsWifCredentials(
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        audience = "https://example.com",
        containerImageSignatureKeyIds = listOf("signing-key"),
      )
    val aead =
      factory
        .getKmsClient(config)
        .getAead("aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key")
    repeat(2) {
      val failure = assertFailsWith<KmsKeyAccessException> { aead.encrypt(ByteArray(0), null) }
      assertThat(failure.isRetryable).isTrue()
      assertThat(failure.cause).isSameInstanceAs(original)
    }
    assertThat(tokenRequests).isEqualTo(2)
  }

  @Test
  fun `encrypt leaves a generic token provider failure unclassified`() {
    val original = IOException("token unavailable")
    val factory =
      ConfidentialSpaceToAwsKmsClientFactory(
        tokenProvider = AttestationTokenProvider { CompletableFuture.failedFuture(original) }
      )
    val config =
      ConfidentialSpaceToAwsWifCredentials(
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        audience = "https://example.com",
        containerImageSignatureKeyIds = listOf("signing-key"),
      )
    val aead =
      factory
        .getKmsClient(config)
        .getAead("aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key")
    val failure = assertFailsWith<KmsKeyAccessException> { aead.encrypt(ByteArray(0), null) }
    assertThat(failure.isRetryable).isFalse()
    assertThat(failure.cause).isSameInstanceAs(original)
  }

  @Test
  fun `decrypt preserves nonretryable attestation-provider failure`() {
    val original = IllegalArgumentException("invalid attestation configuration")
    val provider = AttestationTokenProvider { CompletableFuture.failedFuture(original) }
    val factory = ConfidentialSpaceToAwsKmsClientFactory(tokenProvider = provider)
    val config =
      ConfidentialSpaceToAwsWifCredentials(
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        audience = "https://example.com",
        containerImageSignatureKeyIds = listOf("signing-key"),
      )
    val aead =
      factory
        .getKmsClient(config)
        .getAead("aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key")
    val failure = assertFailsWith<KmsKeyAccessException> { aead.decrypt(ByteArray(0), null) }
    assertThat(failure.isRetryable).isFalse()
    assertThat(failure.cause).isSameInstanceAs(original)
  }
}

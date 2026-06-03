// Copyright 2025 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

/**
 * Tests for [GCloudToAwsKmsClientFactory].
 *
 * Full integration testing requires a Confidential Space environment with a configured Google
 * Cloud/AWS trust relationship. Unit tests verify the refresh logic and that the factory fails
 * gracefully with a bogus config.
 */
@RunWith(JUnit4::class)
class GCloudToAwsKmsClientFactoryTest {

  @Test
  fun `getKmsClient with invalid config fails on first use`() {
    val factory = GCloudToAwsKmsClientFactory()
    val config =
      GCloudToAwsWifCredentials(
        gcloudAudience =
          "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        subjectTokenType = "urn:ietf:params:oauth:token-type:jwt",
        tokenUrl = "https://sts.googleapis.com/v1/token",
        credentialSourceFilePath = "/run/container_launcher/attestation_verifier_claims_token",
        serviceAccountImpersonationUrl =
          "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com:generateAccessToken",
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        awsAudience = "https://example.com/oidc",
      )
    val kmsClient = factory.getKmsClient(config)
    assertFails { kmsClient.getAead("aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key") }
  }

  @Test
  fun `refreshable provider returns cached credentials when not expired`() {
    var callCount = 0
    val credentials = AwsSessionCredentials.create("key", "secret", "token")
    val expiration = Instant.now().plusSeconds(3600)

    val provider =
      GCloudToAwsKmsClientFactory.RefreshableGCloudToAwsCredentialsProvider(
        refreshMarginSeconds = 300
      ) {
        callCount++
        credentials to expiration
      }

    val first = provider.resolveCredentials()
    val second = provider.resolveCredentials()
    val third = provider.resolveCredentials()

    assertThat(first).isEqualTo(credentials)
    assertThat(second).isSameInstanceAs(first)
    assertThat(third).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `refreshable provider refreshes when credentials are near expiry`() {
    var callCount = 0
    val provider =
      GCloudToAwsKmsClientFactory.RefreshableGCloudToAwsCredentialsProvider(
        refreshMarginSeconds = 300
      ) {
        callCount++
        val credentials =
          AwsSessionCredentials.create("key-$callCount", "secret-$callCount", "token-$callCount")
        val expiration = Instant.now().plusSeconds(if (callCount == 1) 60 else 3600)
        credentials to expiration
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")
    assertThat(callCount).isEqualTo(1)

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `refreshable provider refreshes when credentials are already expired`() {
    var callCount = 0
    val provider =
      GCloudToAwsKmsClientFactory.RefreshableGCloudToAwsCredentialsProvider(
        refreshMarginSeconds = 300
      ) {
        callCount++
        val credentials =
          AwsSessionCredentials.create("key-$callCount", "secret-$callCount", "token-$callCount")
        val expiration =
          if (callCount == 1) Instant.now().minusSeconds(100) else Instant.now().plusSeconds(3600)
        credentials to expiration
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `refreshable provider obtains credentials on first call`() {
    var callCount = 0
    val provider =
      GCloudToAwsKmsClientFactory.RefreshableGCloudToAwsCredentialsProvider(
        refreshMarginSeconds = 300
      ) {
        callCount++
        AwsSessionCredentials.create("key", "secret", "token") to Instant.now().plusSeconds(3600)
      }

    assertThat(callCount).isEqualTo(0)
    provider.resolveCredentials()
    assertThat(callCount).isEqualTo(1)
  }

  @Test
  fun `short refresh margin triggers refresh after sleep`() {
    var callCount = 0
    val provider =
      GCloudToAwsKmsClientFactory.RefreshableGCloudToAwsCredentialsProvider(
        refreshMarginSeconds = 2
      ) {
        callCount++
        val credentials =
          AwsSessionCredentials.create("key-$callCount", "secret-$callCount", "token-$callCount")
        val expiration = Instant.now().plusSeconds(3)
        credentials to expiration
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")
    assertThat(callCount).isEqualTo(1)

    Thread.sleep(1500)

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second.accessKeyId()).isEqualTo("key-2")
    assertThat(callCount).isEqualTo(2)
  }

  @Test
  fun `long refresh margin does not trigger refresh after same sleep`() {
    var callCount = 0
    val provider =
      GCloudToAwsKmsClientFactory.RefreshableGCloudToAwsCredentialsProvider(
        refreshMarginSeconds = 2
      ) {
        callCount++
        val credentials =
          AwsSessionCredentials.create("key-$callCount", "secret-$callCount", "token-$callCount")
        val expiration = Instant.now().plusSeconds(3600)
        credentials to expiration
      }

    val first = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(first.accessKeyId()).isEqualTo("key-1")
    assertThat(callCount).isEqualTo(1)

    Thread.sleep(1500)

    val second = provider.resolveCredentials() as AwsSessionCredentials
    assertThat(second).isSameInstanceAs(first)
    assertThat(callCount).isEqualTo(1)
  }
}

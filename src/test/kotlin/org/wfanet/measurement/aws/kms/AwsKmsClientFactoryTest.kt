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

package org.wfanet.measurement.aws.kms

import com.google.common.truth.Truth.assertThat
import java.security.GeneralSecurityException
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.AwsWifCredentials

private const val ROLE_ARN = "arn:aws:iam::123456789012:role/test-role"
private const val ROLE_SESSION_NAME = "test-session"
private const val REGION = "us-east-1"
private const val AWS_KMS_KEY_URI =
  "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key-id"
private const val GCP_KMS_KEY_URI =
  "gcp-kms://projects/test/locations/us/keyRings/kr/cryptoKeys/ck"
private const val FAKE_KMS_KEY_URI = "fake-kms://key1"

/** Tests for [AwsKmsClientFactory]. */
@RunWith(JUnit4::class)
class AwsKmsClientFactoryTest {
  @Rule @JvmField val tempFolder = TemporaryFolder()

  private lateinit var factory: AwsKmsClientFactory

  @Before
  fun setUp() {
    factory = AwsKmsClientFactory()
  }

  private fun createConfig(tokenFilePath: String): AwsWifCredentials {
    return AwsWifCredentials(
      roleArn = ROLE_ARN,
      webIdentityTokenFilePath = tokenFilePath,
      roleSessionName = ROLE_SESSION_NAME,
      region = REGION,
    )
  }

  @Test
  fun `getKmsClient returns KmsClient that supports aws-kms URIs`() {
    val tokenFile = tempFolder.newFile("token.jwt")
    tokenFile.writeText("dummy-oidc-token")
    val config = createConfig(tokenFile.absolutePath)

    val kmsClient = factory.getKmsClient(config)

    assertThat(kmsClient.doesSupport(AWS_KMS_KEY_URI)).isTrue()
  }

  @Test
  fun `getKmsClient returns KmsClient that does not support gcp-kms URIs`() {
    val tokenFile = tempFolder.newFile("token.jwt")
    tokenFile.writeText("dummy-oidc-token")
    val config = createConfig(tokenFile.absolutePath)

    val kmsClient = factory.getKmsClient(config)

    assertThat(kmsClient.doesSupport(GCP_KMS_KEY_URI)).isFalse()
  }

  @Test
  fun `getKmsClient returns KmsClient that does not support fake-kms URIs`() {
    val tokenFile = tempFolder.newFile("token.jwt")
    tokenFile.writeText("dummy-oidc-token")
    val config = createConfig(tokenFile.absolutePath)

    val kmsClient = factory.getKmsClient(config)

    assertThat(kmsClient.doesSupport(FAKE_KMS_KEY_URI)).isFalse()
  }

  @Test
  fun `getKmsClient returns KmsClient whose getAead throws for unsupported URI`() {
    val tokenFile = tempFolder.newFile("token.jwt")
    tokenFile.writeText("dummy-oidc-token")
    val config = createConfig(tokenFile.absolutePath)

    val kmsClient = factory.getKmsClient(config)

    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(GCP_KMS_KEY_URI) }
  }
}

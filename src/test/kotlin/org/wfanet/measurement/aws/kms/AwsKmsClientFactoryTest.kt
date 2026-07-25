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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider

private const val AWS_KMS_KEY_URI = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key-id"
private const val GCP_KMS_KEY_URI = "gcp-kms://projects/test/locations/us/keyRings/kr/cryptoKeys/ck"
private const val FAKE_KMS_KEY_URI = "fake-kms://key1"
private const val INVALID_ARN_KEY_URI = "aws-kms://invalid-arn"

/** Tests for [AwsKmsClient]. */
@RunWith(JUnit4::class)
class AwsKmsClientFactoryTest {
  private lateinit var kmsClient: AwsKmsClient

  @Before
  fun setUp() {
    val credentialsProvider =
      StaticCredentialsProvider.create(AwsBasicCredentials.create("fake-key", "fake-secret"))
    kmsClient = AwsKmsClient(credentialsProvider)
  }

  @Test
  fun `doesSupport returns true for aws-kms URIs`() {
    assertThat(kmsClient.doesSupport(AWS_KMS_KEY_URI)).isTrue()
  }

  @Test
  fun `doesSupport returns false for gcp-kms URIs`() {
    assertThat(kmsClient.doesSupport(GCP_KMS_KEY_URI)).isFalse()
  }

  @Test
  fun `doesSupport returns false for fake-kms URIs`() {
    assertThat(kmsClient.doesSupport(FAKE_KMS_KEY_URI)).isFalse()
  }

  @Test
  fun `doesSupport returns false for null URI`() {
    assertThat(kmsClient.doesSupport(null)).isFalse()
  }

  @Test
  fun `getAead throws GeneralSecurityException for unsupported URI`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(GCP_KMS_KEY_URI) }
  }

  @Test
  fun `getAead throws GeneralSecurityException for null URI`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(null) }
  }

  @Test
  fun `getAead throws GeneralSecurityException for invalid ARN`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(INVALID_ARN_KEY_URI) }
  }
}

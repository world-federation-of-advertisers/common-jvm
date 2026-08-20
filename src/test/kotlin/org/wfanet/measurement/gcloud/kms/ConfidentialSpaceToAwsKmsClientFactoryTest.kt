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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.aws.kms.ExceptionTranslatingKmsClient
import org.wfanet.measurement.common.crypto.tink.ConfidentialSpaceToAwsWifCredentials

/**
 * Tests for [ConfidentialSpaceToAwsKmsClientFactory].
 *
 * Full integration testing requires a Confidential Space environment with a configured AWS trust
 * relationship. This unit test verifies that the factory fails gracefully with a bogus config. The
 * token-fetch path is covered by
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
    assertFails { aead.encrypt(ByteArray(0), null) }
  }

  @Test
  fun `getKmsClient returns an ExceptionTranslatingKmsClient`() {
    val factory = ConfidentialSpaceToAwsKmsClientFactory()
    val config =
      ConfidentialSpaceToAwsWifCredentials(
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        audience = "https://example.com",
      )

    val kmsClient = factory.getKmsClient(config)

    assertThat(kmsClient).isInstanceOf(ExceptionTranslatingKmsClient::class.java)
  }
}

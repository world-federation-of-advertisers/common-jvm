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

import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.GcpToAwsWifCredentials

/**
 * Tests for [GcpToAwsKmsClientFactory].
 *
 * Full integration testing requires a Confidential Space environment with a configured GCP/AWS
 * trust relationship. This unit test verifies that the factory fails gracefully with a bogus config.
 */
@RunWith(JUnit4::class)
class GcpToAwsKmsClientFactoryTest {

  @Test
  fun `getKmsClient fails with invalid config`() {
    val factory = GcpToAwsKmsClientFactory()
    val config =
      GcpToAwsWifCredentials(
        gcpAudience = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        subjectTokenType = "urn:ietf:params:oauth:token-type:jwt",
        tokenUrl = "https://sts.googleapis.com/v1/token",
        credentialSourceFilePath = "/run/container_launcher/attestation_verifier_claims_token",
        serviceAccountImpersonationUrl = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@project.iam.gserviceaccount.com:generateAccessToken",
        roleArn = "arn:aws:iam::123456789012:role/test-role",
        roleSessionName = "test-session",
        region = "us-east-1",
        awsAudience = "https://example.com/oidc",
      )
    assertFails { factory.getKmsClient(config) }
  }
}

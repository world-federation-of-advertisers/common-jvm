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

import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import com.google.auth.http.HttpTransportFactory
import com.google.common.truth.Truth.assertThat
import java.io.IOException
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsKeyAccessException

class GCloudKmsClientFactoryTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `encrypt exposes credential refresh failure before calling KMS`() {
    val response =
      MockLowLevelHttpResponse()
        .setStatusCode(503)
        .setContentType("application/json")
        .setContent("{\"error\":\"temporarily_unavailable\"}")
    val transport = MockHttpTransport.Builder().setLowLevelHttpResponse(response).build()
    val factory = GCloudKmsClientFactory(HttpTransportFactory { transport })
    val token = temporaryFolder.newFile("token").apply { writeText("test-token") }
    val config =
      GCloudWifCredentials(
        audience =
          "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        subjectTokenType = "urn:ietf:params:oauth:token-type:jwt",
        tokenUrl = "https://sts.googleapis.com/v1/token",
        credentialSourceFilePath = token.path,
        serviceAccountImpersonationUrl =
          "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@example.iam.gserviceaccount.com:generateAccessToken",
      )
    val aead =
      factory
        .getKmsClient(config)
        .getAead("gcp-kms://projects/project/locations/global/keyRings/ring/cryptoKeys/key")
    val failure = assertFailsWith<KmsKeyAccessException> { aead.encrypt(ByteArray(0), null) }
    assertThat(failure.cause).isInstanceOf(IOException::class.java)
    // The impersonation layer exposes IOException without a public retry hint.
    assertThat(failure.isRetryable).isTrue()
  }
}

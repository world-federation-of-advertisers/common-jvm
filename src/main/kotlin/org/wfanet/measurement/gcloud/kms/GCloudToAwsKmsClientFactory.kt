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

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdTokenCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import com.google.crypto.tink.KmsClient
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import java.security.GeneralSecurityException
import org.wfanet.measurement.aws.kms.AwsKmsClient
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse
import software.amazon.awssdk.services.sts.model.Credentials

/**
 * A [KmsClientFactory] for accessing AWS KMS from a Google Cloud Confidential Space workload.
 *
 * Uses the same external-account credential flow as [GCloudKmsClientFactory] to exchange a
 * Confidential Space attestation token for Google Cloud credentials, then impersonates a service
 * account to obtain an OIDC ID token. That ID token is exchanged with AWS STS
 * `AssumeRoleWithWebIdentity` for temporary AWS credentials.
 */
class GCloudToAwsKmsClientFactory : KmsClientFactory<GCloudToAwsWifCredentials> {
  /**
   * Returns an [AwsKmsClient] using Google Cloud Confidential Space identity to authenticate with
   * AWS.
   *
   * The flow:
   * 1. Build an `external_account` credential from the attestation token file
   * 2. Impersonate a Google Cloud service account from those credentials
   * 3. Generate an OIDC ID token with the AWS audience
   * 4. Exchange the ID token with AWS STS `AssumeRoleWithWebIdentity`
   *
   * @param config The Google Cloud-to-AWS WIF configuration.
   * @return An initialized [AwsKmsClient].
   * @throws GeneralSecurityException if credentials cannot be obtained or exchanged.
   */
  override fun getKmsClient(config: GCloudToAwsWifCredentials): KmsClient {
    val externalAccountCredentials: GoogleCredentials = buildExternalAccountCredentials(config)

    val impersonatedCredentials: ImpersonatedCredentials =
      ImpersonatedCredentials.newBuilder()
        .apply {
          setSourceCredentials(externalAccountCredentials)
          setTargetPrincipal(extractServiceAccount(config.serviceAccountImpersonationUrl))
          setScopes(listOf("https://www.googleapis.com/auth/cloud-platform"))
        }
        .build()

    val idToken: String =
      try {
        val idTokenCredentials: IdTokenCredentials =
          IdTokenCredentials.newBuilder()
            .apply {
              setIdTokenProvider(impersonatedCredentials)
              setTargetAudience(config.awsAudience)
            }
            .build()
        idTokenCredentials.refresh()
        idTokenCredentials.idToken.tokenValue
      } catch (e: Exception) {
        throw GeneralSecurityException("Failed to obtain Google Cloud ID token", e)
      }

    val stsClient: StsClient =
      try {
        StsClient.builder()
          .apply {
            region(Region.of(config.region))
            credentialsProvider(AnonymousCredentialsProvider.create())
          }
          .build()
      } catch (e: Exception) {
        throw GeneralSecurityException("Failed to create AWS STS client", e)
      }

    val stsResponse: AssumeRoleWithWebIdentityResponse =
      try {
        stsClient.assumeRoleWithWebIdentity(
          AssumeRoleWithWebIdentityRequest.builder()
            .apply {
              roleArn(config.roleArn)
              roleSessionName(config.roleSessionName)
              webIdentityToken(idToken)
            }
            .build()
        )
      } catch (e: Exception) {
        throw GeneralSecurityException("AWS STS AssumeRoleWithWebIdentity failed", e)
      }

    val awsCredentials: Credentials = stsResponse.credentials()
    val credentialsProvider: StaticCredentialsProvider =
      StaticCredentialsProvider.create(
        AwsSessionCredentials.create(
          awsCredentials.accessKeyId(),
          awsCredentials.secretAccessKey(),
          awsCredentials.sessionToken(),
        )
      )

    return AwsKmsClient(credentialsProvider)
  }

  companion object {
    private fun buildExternalAccountCredentials(
      config: GCloudToAwsWifCredentials
    ): GoogleCredentials {
      val wifConfigJson: String =
        JsonObject().run {
          addProperty("type", "external_account")
          addProperty("audience", config.gcloudAudience)
          addProperty("subject_token_type", config.subjectTokenType)
          addProperty("token_url", config.tokenUrl)
          add(
            "credential_source",
            JsonObject().apply { addProperty("file", config.credentialSourceFilePath) },
          )
          addProperty("service_account_impersonation_url", config.serviceAccountImpersonationUrl)
          add("scopes", JsonArray().apply { add("https://www.googleapis.com/auth/cloud-platform") })
          toString()
        }

      try {
        return GoogleCredentials.fromStream(wifConfigJson.byteInputStream(Charsets.UTF_8))
      } catch (e: Exception) {
        throw GeneralSecurityException("Failed to create GoogleCredentials from WIF config", e)
      }
    }

    private fun extractServiceAccount(impersonationUrl: String): String {
      val regex = Regex("serviceAccounts/([^:/]+)")
      return regex.find(impersonationUrl)?.groupValues?.get(1)
        ?: throw GeneralSecurityException(
          "Cannot extract service account from impersonation URL: $impersonationUrl"
        )
    }
  }
}

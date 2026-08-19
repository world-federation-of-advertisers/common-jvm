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

@file:Suppress(
  "DEPRECATION"
) // Uses the deprecated custom AwsKmsClient during its transition period.

package org.wfanet.measurement.gcloud.kms

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdTokenCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.awskms.AwsKmsClient as TinkAwsKmsClient
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import java.security.GeneralSecurityException
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger
import org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider
import org.wfanet.measurement.aws.TimeBoundCredentials
import org.wfanet.measurement.aws.kms.AssociatedDataEncoding
import org.wfanet.measurement.aws.kms.AwsKmsClient
import org.wfanet.measurement.aws.kms.ExceptionTranslatingKmsClient
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest

/**
 * A [KmsClientFactory] for accessing AWS KMS from a Google Cloud Confidential Space workload.
 *
 * Uses the same external-account credential flow as [GCloudKmsClientFactory] to exchange a
 * Confidential Space attestation token for Google Cloud credentials, then impersonates a service
 * account to obtain an OIDC ID token. That ID token is exchanged with AWS STS
 * `AssumeRoleWithWebIdentity` for temporary AWS credentials.
 *
 * The credentials obtained this way are exposed through [RefreshableAwsCredentialsProvider], which
 * implements both the AWS SDK's async `IdentityProvider<AwsCredentialsIdentity>` contract and the
 * older, blocking `AwsCredentialsProvider` contract that upstream `tink-awskms`'s public
 * `AwsKmsClient.withCredentialsProvider` method requires. See [RefreshableAwsCredentialsProvider]'s
 * class documentation for why both are implemented and the tradeoffs of each.
 *
 * @param refreshMargin How far before expiration to proactively refresh credentials.
 * @param clock Clock used to determine the current time.
 * @param useLegacyBase64Encoding If `true`, returns the deprecated custom
 *   [org.wfanet.measurement.aws.kms.AwsKmsClient] configured with [AssociatedDataEncoding.BASE64],
 *   for decrypting ciphertext written by older versions of this client. Defaults to `false`, which
 *   returns the upstream `tink-awskms` client; do not set this for new usages.
 */
class GCloudToAwsKmsClientFactory(
  private val refreshMargin: Duration = DEFAULT_REFRESH_MARGIN,
  private val clock: Clock = Clock.systemUTC(),
  private val useLegacyBase64Encoding: Boolean = false,
) : KmsClientFactory<GCloudToAwsWifCredentials> {
  /**
   * Returns a [KmsClient] using Google Cloud Confidential Space identity to authenticate with AWS.
   *
   * The returned client uses a credentials provider that automatically refreshes the AWS session
   * credentials before they expire by re-executing the full credential chain (GCP attestation ->
   * service account impersonation -> OIDC ID token -> AWS STS AssumeRoleWithWebIdentity).
   *
   * @param config The Google Cloud-to-AWS WIF configuration.
   * @return An initialized [KmsClient] — the upstream `tink-awskms` client (wrapped in
   *   [org.wfanet.measurement.aws.kms.ExceptionTranslatingKmsClient] so credential-refresh failures
   *   surface as [GeneralSecurityException], matching the deprecated client's behavior), or the
   *   deprecated custom [org.wfanet.measurement.aws.kms.AwsKmsClient] with
   *   [AssociatedDataEncoding.BASE64] if [useLegacyBase64Encoding] is `true`.
   * @throws GeneralSecurityException if credentials cannot be obtained or exchanged.
   */
  override fun getKmsClient(config: GCloudToAwsWifCredentials): KmsClient {
    val credentialsProvider =
      RefreshableAwsCredentialsProvider(refreshMargin = refreshMargin, clock = clock) {
        // The credential chain is blocking, and the AWS SDK resolves credentials from a
        // thread where blocking is expected, so it runs inline rather than on another thread.
        CompletableFuture.completedFuture(obtainAwsCredentials(config))
      }
    return if (useLegacyBase64Encoding) {
      @Suppress("DEPRECATION") AwsKmsClient(credentialsProvider, AssociatedDataEncoding.BASE64)
    } else {
      // TinkAwsKmsClient.withCredentialsProvider requires the AwsCredentialsProvider type,
      // which RefreshableAwsCredentialsProvider now also implements (see its class doc for why).
      // The default here favors the upstream client because HEX-encoded ciphertext from either
      // client is mutually interoperable, so there is no reason to prefer the deprecated one
      // unless BASE64 decoding is actually needed.
      //
      // Wrapped in ExceptionTranslatingKmsClient because a failed credential refresh here
      // surfaces as a raw CompletionException that upstream's own exception translation doesn't
      // recognize (see that class's doc) — without this wrapper, callers expecting
      // GeneralSecurityException from Aead operations would see an unhandled exception instead.
      ExceptionTranslatingKmsClient(TinkAwsKmsClient().withCredentialsProvider(credentialsProvider))
    }
  }

  companion object {
    private val DEFAULT_REFRESH_MARGIN: Duration = Duration.ofMinutes(15)

    private val SERVICE_ACCOUNT_REGEX = Regex("serviceAccounts/([^:/]+)")

    private val logger: Logger = Logger.getLogger(this::class.java.enclosingClass.name)

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
      return SERVICE_ACCOUNT_REGEX.find(impersonationUrl)?.groupValues?.get(1)
        ?: throw GeneralSecurityException(
          "Cannot extract service account from impersonation URL: $impersonationUrl"
        )
    }

    private fun obtainAwsCredentials(config: GCloudToAwsWifCredentials): TimeBoundCredentials {
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

      val stsResponse =
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
        } finally {
          stsClient.close()
        }

      val awsCredentials = stsResponse.credentials()
      return TimeBoundCredentials(
        credentials =
          AwsSessionCredentials.create(
            awsCredentials.accessKeyId(),
            awsCredentials.secretAccessKey(),
            awsCredentials.sessionToken(),
          ),
        expiration = awsCredentials.expiration(),
      )
    }
  }
}

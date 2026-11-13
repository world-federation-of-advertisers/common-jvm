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

import com.google.api.client.http.HttpResponseException
import com.google.auth.Retryable
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
import java.util.concurrent.ThreadLocalRandom
import org.wfanet.measurement.aws.AwsCredentialsProviderAdapter
import org.wfanet.measurement.aws.RefreshableAwsCredentialsIdentityProvider
import org.wfanet.measurement.aws.TimeBoundCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest
import software.amazon.awssdk.services.sts.model.StsException

/**
 * A [KmsClientFactory] for accessing AWS KMS from a Google Cloud Confidential Space workload.
 *
 * Uses the same external-account credential flow as [GCloudKmsClientFactory] to exchange a
 * Confidential Space attestation token for Google Cloud credentials, then impersonates a service
 * account to obtain an OIDC ID token. That ID token is exchanged with AWS STS
 * `AssumeRoleWithWebIdentity` for temporary AWS credentials.
 *
 * Credential retrieval retries only public Google authentication retry hints and transient HTTP or
 * STS failures, with at most three retries and a two-minute budget for the whole refresh. Quota
 * failures wait at least one minute. STS retries reuse the Google ID token already obtained. Errors
 * without a public retry hint, including opaque impersonation I/O failures, are not retried.
 *
 * @param refreshMargin How far before expiration to proactively refresh credentials.
 * @param clock Clock used to determine the current time.
 */
class GCloudToAwsKmsClientFactory(
  private val refreshMargin: Duration = DEFAULT_REFRESH_MARGIN,
  private val clock: Clock = Clock.systemUTC(),
) : KmsClientFactory<GCloudToAwsWifCredentials> {
  /**
   * Returns a [KmsClient] using Google Cloud Confidential Space identity to authenticate with AWS.
   *
   * The returned client uses a credentials provider that automatically refreshes the AWS session
   * credentials before they expire by re-executing the full credential chain (GCP attestation ->
   * service account impersonation -> OIDC ID token -> AWS STS AssumeRoleWithWebIdentity).
   *
   * @param config The Google Cloud-to-AWS WIF configuration.
   * @return An initialized [KmsClient].
   * @throws GeneralSecurityException if credentials cannot be obtained or exchanged.
   */
  override fun getKmsClient(config: GCloudToAwsWifCredentials): KmsClient {
    val credentialsProvider =
      RefreshableAwsCredentialsIdentityProvider(refreshMargin = refreshMargin, clock = clock) {
        obtainAwsCredentials(config).timeout(CREDENTIAL_REFRESH_TIMEOUT).toFuture()
      }
    return TinkAwsKmsClient()
      .withCredentialsProvider(AwsCredentialsProviderAdapter(credentialsProvider))
  }

  companion object {
    private const val MAX_CREDENTIAL_RETRIES = 3L
    private val CREDENTIAL_REFRESH_TIMEOUT = Duration.ofMinutes(2)
    private val QUOTA_BACKOFF = Duration.ofMinutes(1)

    private val DEFAULT_REFRESH_MARGIN: Duration = Duration.ofMinutes(15)

    private val SERVICE_ACCOUNT_REGEX = Regex("serviceAccounts/([^:/]+)")

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

    private fun obtainAwsCredentials(
      config: GCloudToAwsWifCredentials
    ): Mono<TimeBoundCredentials> =
      Mono.fromCallable { googleIdToken(config) }
        .subscribeOn(Schedulers.boundedElastic())
        .retryWhen(credentialRetry())
        .onErrorMap { e -> GeneralSecurityException("Failed to obtain Google Cloud ID token", e) }
        .flatMap { idToken ->
          Mono.fromCallable { exchangeForAwsCredentials(config, idToken) }
            .subscribeOn(Schedulers.boundedElastic())
            .retryWhen(credentialRetry())
            .onErrorMap { e ->
              GeneralSecurityException("AWS STS AssumeRoleWithWebIdentity failed", e)
            }
        }

    private fun googleIdToken(config: GCloudToAwsWifCredentials): String {
      val externalAccountCredentials: GoogleCredentials = buildExternalAccountCredentials(config)

      val impersonatedCredentials: ImpersonatedCredentials =
        ImpersonatedCredentials.newBuilder()
          .apply {
            setSourceCredentials(externalAccountCredentials)
            setTargetPrincipal(extractServiceAccount(config.serviceAccountImpersonationUrl))
            setScopes(listOf("https://www.googleapis.com/auth/cloud-platform"))
          }
          .build()

      val idTokenCredentials: IdTokenCredentials =
        IdTokenCredentials.newBuilder()
          .apply {
            setIdTokenProvider(impersonatedCredentials)
            setTargetAudience(config.awsAudience)
          }
          .build()
      idTokenCredentials.refresh()
      return idTokenCredentials.idToken.tokenValue
    }

    private fun exchangeForAwsCredentials(
      config: GCloudToAwsWifCredentials,
      idToken: String,
    ): TimeBoundCredentials {
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

    /** Retries only failures with public retry hints; quota backoff covers a full minute. */
    private fun credentialRetry(): Retry =
      Retry.from { signals ->
        signals.concatMap { signal ->
          val failure = signal.failure()
          val retryable =
            when (failure) {
              is HttpResponseException -> isRetryableHttpStatus(failure.statusCode)
              is StsException ->
                failure.isThrottlingException || isRetryableHttpStatus(failure.statusCode())
              is Retryable -> failure.isRetryable
              else -> false
            }
          if (!retryable || signal.totalRetries() >= MAX_CREDENTIAL_RETRIES) {
            Mono.error<Long>(failure)
          } else {
            val quotaFailure =
              (failure is HttpResponseException && failure.statusCode == 429) ||
                (failure is StsException && failure.isThrottlingException)
            val delay =
              if (quotaFailure) QUOTA_BACKOFF
              else Duration.ofSeconds(1L shl signal.totalRetries().toInt())
            Mono.delay(delay.plusMillis(ThreadLocalRandom.current().nextLong(1000)))
          }
        }
      }

    private fun isRetryableHttpStatus(statusCode: Int): Boolean =
      when (statusCode) {
        408,
        429,
        500,
        502,
        503,
        504 -> true
        else -> false
      }
  }
}

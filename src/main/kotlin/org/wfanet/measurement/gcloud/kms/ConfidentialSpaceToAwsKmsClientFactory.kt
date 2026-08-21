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

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.awskms.AwsKmsClient as TinkAwsKmsClient
import java.security.GeneralSecurityException
import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.aws.AwsCredentialsProviderAdapter
import org.wfanet.measurement.aws.RefreshableAwsCredentialsProvider
import org.wfanet.measurement.aws.TimeBoundCredentials
import org.wfanet.measurement.aws.kms.ExceptionTranslatingKmsClient
import org.wfanet.measurement.common.crypto.tink.ConfidentialSpaceToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import org.wfanet.measurement.gcloud.confidentialspace.AttestationTokenProvider
import org.wfanet.measurement.gcloud.confidentialspace.AttestationTokenRequest
import org.wfanet.measurement.gcloud.confidentialspace.ConfidentialSpaceTokenClient
import org.wfanet.measurement.gcloud.confidentialspace.ConfidentialSpaceTokenType
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest

/**
 * A [KmsClientFactory] for accessing AWS KMS directly from a Google Cloud Confidential Space
 * workload, with no intermediary Google Cloud Workload Identity pool or service account.
 *
 * Requests an `AWS_PRINCIPALTAGS` attestation token from the Confidential Space launcher (via
 * [tokenProvider]) and exchanges it with AWS STS `AssumeRoleWithWebIdentity` for temporary AWS
 * credentials. The token carries the workload's attestation claims as AWS session tags, so the IAM
 * role's trust policy gates access on them directly (the role must permit `sts:TagSession` in
 * addition to `sts:AssumeRoleWithWebIdentity`).
 *
 * The returned client uses a credentials provider that automatically refreshes the AWS session
 * credentials before they expire by re-executing the token fetch + STS exchange.
 *
 * The credentials obtained this way are exposed through [RefreshableAwsCredentialsProvider],
 * wrapped in [AwsCredentialsProviderAdapter] to satisfy the `AwsCredentialsProvider` type upstream
 * `tink-awskms`'s public `AwsKmsClient.withCredentialsProvider` method currently requires.
 *
 * @param tokenProvider Source of Confidential Space attestation tokens.
 * @param refreshMargin How far before expiration to proactively refresh credentials.
 * @param clock Clock used to determine the current time.
 */
class ConfidentialSpaceToAwsKmsClientFactory(
  private val tokenProvider: AttestationTokenProvider = ConfidentialSpaceTokenClient(),
  private val refreshMargin: Duration = DEFAULT_REFRESH_MARGIN,
  private val clock: Clock = Clock.systemUTC(),
) : KmsClientFactory<ConfidentialSpaceToAwsWifCredentials> {
  /**
   * Returns a [KmsClient] using a Confidential Space attestation token to authenticate directly
   * with AWS.
   *
   * The token fetch and STS exchange are deferred until the AWS SDK resolves credentials, and fail
   * that resolution with a [GeneralSecurityException] when they cannot be completed.
   *
   * @param config The Confidential Space-to-AWS configuration.
   * @return An initialized [KmsClient] wrapping the upstream `tink-awskms` client — see
   *   [ExceptionTranslatingKmsClient] for the wrapping's contract.
   */
  override fun getKmsClient(config: ConfidentialSpaceToAwsWifCredentials): KmsClient {
    val credentialsProvider =
      RefreshableAwsCredentialsProvider(refreshMargin = refreshMargin, clock = clock) {
        obtainAwsCredentials(config).toFuture()
      }
    return ExceptionTranslatingKmsClient(
      TinkAwsKmsClient()
        .withCredentialsProvider(
          // TODO(tink-crypto/tink-java-awskms#6): once a release including the fix
          // (tink-crypto/tink-java-awskms#8) is available, pass credentialsProvider directly
          // instead of wrapping it in AwsCredentialsProviderAdapter.
          AwsCredentialsProviderAdapter(credentialsProvider)
        )
    )
  }

  private fun obtainAwsCredentials(
    config: ConfidentialSpaceToAwsWifCredentials
  ): Mono<TimeBoundCredentials> =
    resolveSignatureKeyIds(config)
      .flatMap { signatureKeyIds -> attestationToken(config, signatureKeyIds) }
      .flatMap { attestationToken -> exchangeForAwsCredentials(config, attestationToken) }
      .onErrorMap { e ->
        if (e is GeneralSecurityException) e
        else GeneralSecurityException("Failed to obtain AWS credentials", e)
      }

  /**
   * Resolves the container image signature key IDs to request as AWS principal tags.
   *
   * When none are configured they are discovered from the workload's own OIDC attestation token, by
   * reading its `submods.container.image_signatures` claim, so no key IDs need to be hardcoded
   * anywhere. The AWS role trust policy remains the authority on which signers are acceptable.
   */
  private fun resolveSignatureKeyIds(
    config: ConfidentialSpaceToAwsWifCredentials
  ): Mono<List<String>> {
    if (config.containerImageSignatureKeyIds.isNotEmpty()) {
      return Mono.just(config.containerImageSignatureKeyIds)
    }
    return Mono.fromFuture {
        tokenProvider.getToken(
          AttestationTokenRequest(
            audience = config.audience,
            tokenType = ConfidentialSpaceTokenType.OIDC,
          )
        )
      }
      .onErrorMap { e ->
        GeneralSecurityException(
          "Failed to obtain Confidential Space OIDC token for signature discovery",
          e,
        )
      }
      .map { oidcToken ->
        ConfidentialSpaceTokenClient.parseContainerImageSignatureKeyIds(oidcToken)
      }
  }

  private fun attestationToken(
    config: ConfidentialSpaceToAwsWifCredentials,
    signatureKeyIds: List<String>,
  ): Mono<String> =
    Mono.fromFuture {
        tokenProvider.getToken(
          AttestationTokenRequest(
            audience = config.audience,
            tokenType = ConfidentialSpaceTokenType.AWS_PRINCIPAL_TAGS,
            containerImageSignatureKeyIds = signatureKeyIds,
          )
        )
      }
      .onErrorMap { e ->
        GeneralSecurityException("Failed to obtain Confidential Space attestation token", e)
      }

  /**
   * Exchanges [attestationToken] with AWS STS for temporary AWS credentials.
   *
   * [StsClient] is blocking, so the exchange is scheduled on [Schedulers.boundedElastic] rather
   * than on the event loop that delivered the attestation token.
   */
  private fun exchangeForAwsCredentials(
    config: ConfidentialSpaceToAwsWifCredentials,
    attestationToken: String,
  ): Mono<TimeBoundCredentials> =
    Mono.fromCallable { assumeRoleWithWebIdentity(config, attestationToken) }
      .subscribeOn(Schedulers.boundedElastic())

  private fun assumeRoleWithWebIdentity(
    config: ConfidentialSpaceToAwsWifCredentials,
    attestationToken: String,
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
      stsClient.use {
        try {
          it.assumeRoleWithWebIdentity(
            AssumeRoleWithWebIdentityRequest.builder()
              .apply {
                roleArn(config.roleArn)
                roleSessionName(config.roleSessionName)
                webIdentityToken(attestationToken)
              }
              .build()
          )
        } catch (e: Exception) {
          throw GeneralSecurityException("AWS STS AssumeRoleWithWebIdentity failed", e)
        }
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

  companion object {
    private val DEFAULT_REFRESH_MARGIN: Duration = Duration.ofMinutes(15)
  }
}

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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.awskms.AwsKmsClient as TinkAwsKmsClient
import java.nio.file.Paths
import java.security.GeneralSecurityException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import org.wfanet.measurement.aws.AwsCredentialsProviderAdapter
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider

/** A [KmsClientFactory] for creating Tink [KmsClient] instances for AWS KMS. */
class AwsKmsClientFactory : KmsClientFactory<AwsWebIdentityCredentials> {
  /**
   * Returns a [KmsClient] configured via STS AssumeRoleWithWebIdentity.
   *
   * This method creates an [StsWebIdentityTokenFileCredentialsProvider] that exchanges a web
   * identity token (e.g., an OIDC token from a Kubernetes service account) for temporary AWS
   * credentials by assuming an IAM role. The STS client uses [AnonymousCredentialsProvider] because
   * `AssumeRoleWithWebIdentity` authenticates via the web identity token itself and does not
   * require pre-existing AWS credentials. This allows the factory to be used from non-AWS
   * environments (e.g., Google Cloud).
   *
   * @param config The AWS web identity configuration.
   * @return An initialized [KmsClient] whose [Aead] instances do not throw [CompletionException].
   * @throws GeneralSecurityException if the client cannot be initialized.
   */
  override fun getKmsClient(config: AwsWebIdentityCredentials): KmsClient {
    val stsClient =
      try {
        StsClient.builder()
          .apply {
            region(Region.of(config.region))
            credentialsProvider(AnonymousCredentialsProvider.create())
          }
          .build()
      } catch (e: Exception) {
        throw GeneralSecurityException("Failed to create STS client", e)
      }

    val stsCredentialsProvider =
      StsWebIdentityTokenFileCredentialsProvider.builder()
        .apply {
          stsClient(stsClient)
          roleArn(config.roleArn)
          roleSessionName(config.roleSessionName)
          webIdentityTokenFile(Paths.get(config.webIdentityTokenFilePath))
        }
        .build()

    // StsWebIdentityTokenFileCredentialsProvider resolves synchronously under the hood via its
    // inherited resolveIdentity default (which calls resolveCredentials eagerly), so a failure
    // such as a missing web identity token file throws directly instead of failing the returned
    // future.
    val credentialsProvider =
      object : IdentityProvider<AwsCredentialsIdentity> by stsCredentialsProvider {
        override fun resolveIdentity(
          request: ResolveIdentityRequest
        ): CompletableFuture<AwsCredentialsIdentity> =
          try {
            // .thenApply { it } adapts the Java wildcard return type
            // (CompletableFuture<? extends AwsCredentialsIdentity>) to the invariant type Kotlin
            // requires for this override's signature.
            stsCredentialsProvider.resolveIdentity(request).thenApply { it }
          } catch (e: Exception) {
            CompletableFuture.failedFuture(
              GeneralSecurityException("Failed to resolve AWS credentials", e)
            )
          }
      }

    return CompletionExceptionTranslatingKmsClient.wrap(
      TinkAwsKmsClient()
        .withCredentialsProvider(
          // TODO(tink-crypto/tink-java-awskms#6): once a release including the fix is
          // available, pass credentialsProvider directly instead of wrapping it in
          // AwsCredentialsProviderAdapter.
          AwsCredentialsProviderAdapter(credentialsProvider)
        )
    )
  }
}

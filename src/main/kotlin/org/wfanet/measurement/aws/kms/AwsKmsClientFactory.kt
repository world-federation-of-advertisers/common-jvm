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

import com.google.crypto.tink.KmsClient
import java.nio.file.Paths
import java.security.GeneralSecurityException
import org.wfanet.measurement.common.crypto.tink.AwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider

/** A [KmsClientFactory] for creating Tink [KmsClient] instances for AWS KMS. */
class AwsKmsClientFactory : KmsClientFactory<AwsWifCredentials> {
  /**
   * Returns an [AwsKmsClient] configured for Workload Identity Federation via STS
   * AssumeRoleWithWebIdentity.
   *
   * This method creates an [StsWebIdentityTokenFileCredentialsProvider] that exchanges a web
   * identity token (e.g., an OIDC token from a Kubernetes service account) for temporary AWS
   * credentials by assuming an IAM role. The STS client uses [AnonymousCredentialsProvider] because
   * `AssumeRoleWithWebIdentity` authenticates via the web identity token itself and does not require
   * pre-existing AWS credentials. This allows the factory to be used from non-AWS environments
   * (e.g., Google Cloud).
   *
   * @param config The AWS specific WIF configuration.
   * @return An initialized [AwsKmsClient].
   * @throws GeneralSecurityException if the client cannot be initialized.
   */
  override fun getKmsClient(config: AwsWifCredentials): KmsClient {
    val stsClient =
      try {
        StsClient.builder()
          .region(Region.of(config.region))
          .credentialsProvider(AnonymousCredentialsProvider.create())
          .build()
      } catch (e: Exception) {
        throw GeneralSecurityException("Failed to create STS client", e)
      }

    val credentialsProvider =
      StsWebIdentityTokenFileCredentialsProvider.builder()
        .stsClient(stsClient)
        .roleArn(config.roleArn)
        .roleSessionName(config.roleSessionName)
        .webIdentityTokenFile(Paths.get(config.webIdentityTokenFilePath))
        .build()

    return AwsKmsClient(credentialsProvider)
  }
}

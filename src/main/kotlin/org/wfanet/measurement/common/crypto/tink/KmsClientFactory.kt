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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.KmsClient
import java.security.GeneralSecurityException

/** A sealed interface for Workload Identity Federation (WIF) configurations. */
sealed interface WifCredentials

/**
 * Configuration for creating credentials using GCP's Workload Identity Federation (WIF) and service
 * account impersonation.
 */
data class GCloudWifCredentials(
  /** The audience for the WIF token exchange. */
  val audience: String,
  /** The type of the token being presented (e.g., an OIDC token type). */
  val subjectTokenType: String,
  /** The Security Token Service (STS) token endpoint URL. */
  val tokenUrl: String,
  /** The file path to the subject token (e.g., an attestation token). */
  val credentialSourceFilePath: String,
  /** The URL to impersonate a service account to get a final access token. */
  val serviceAccountImpersonationUrl: String,
) : WifCredentials

/**
 * Configuration for creating credentials using AWS's Workload Identity Federation via STS
 * AssumeRoleWithWebIdentity.
 */
data class AwsWifCredentials(
  /** The ARN of the IAM role to assume. */
  val roleArn: String,
  /** The file path to the web identity token (e.g., an OIDC token). */
  val webIdentityTokenFilePath: String,
  /** An identifier for the assumed role session. */
  val roleSessionName: String,
  /** The AWS region for the STS endpoint. */
  val region: String,
) : WifCredentials

/**
 * Factory for creating [KmsClient] instances.
 *
 * @param T The specific type of [WifCredentials] this factory supports.
 */
interface KmsClientFactory<T : WifCredentials> {
  /**
   * Returns a [KmsClient] instance for a specific [WifCredentials] configuration.
   *
   * @param config The WIF configuration.
   * @return An initialized [KmsClient].
   * @throws GeneralSecurityException if the client cannot be initialized.
   */
  fun getKmsClient(config: T): KmsClient
}

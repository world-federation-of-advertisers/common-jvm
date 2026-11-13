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
import com.google.auth.http.HttpTransportFactory
import com.google.auth.oauth2.GoogleCredentials
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import java.io.IOException
import java.security.GeneralSecurityException
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import org.wfanet.measurement.common.crypto.tink.KmsKeyAccessException
import org.wfanet.measurement.common.crypto.tink.internal.KmsClientWithRetryInfo

/**
 * [KmsClientFactory] for Google Cloud KMS with documented retry information.
 *
 * Credentials are refreshed before key use. Public HTTP status and [Retryable] hints determine
 * retries when available. Unclassified failures have no retry recommendation, and key-operation
 * errors pass through unchanged.
 */
class GCloudKmsClientFactory
private constructor(private val httpTransportFactory: HttpTransportFactory?) :
  KmsClientFactory<GCloudWifCredentials> {
  constructor() : this(null)

  /**
   * Returns a [KmsClient] backed by [GcpKmsClient] configured for Workload Identity Federation
   * (WIF) with service account impersonation.
   *
   * This method programmatically builds an `ExternalAccountCredentials` object, which is ideal for
   * environments like Confidential Space where a short-lived token from a file needs to be
   * exchanged for a GCP access token.
   *
   * @param config The Google Cloud specific WIF and impersonation configuration.
   * @return An initialized [KmsClient] exposing credential retry hints via [KmsKeyAccessException].
   * @throws GeneralSecurityException if the client cannot be initialized.
   */
  override fun getKmsClient(config: GCloudWifCredentials): KmsClient {
    val wifConfigJson =
      JsonObject().run {
        addProperty("type", "external_account")
        addProperty("audience", config.audience)
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

    val credentials =
      try {
        if (httpTransportFactory == null) {
          GoogleCredentials.fromStream(wifConfigJson.byteInputStream(Charsets.UTF_8))
        } else {
          GoogleCredentials.fromStream(
            wifConfigJson.byteInputStream(Charsets.UTF_8),
            httpTransportFactory,
          )
        }
      } catch (e: IOException) {
        throw KmsKeyAccessException("Failed to create GoogleCredentials from WIF config", false, e)
      }

    val client =
      try {
        GcpKmsClient().withCredentials(credentials)
      } catch (e: GeneralSecurityException) {
        throw KmsKeyAccessException("Failed to initialize Google Cloud KMS client", false, e)
      }
    return KmsClientWithRetryInfo(client) {
      try {
        credentials.refreshIfExpired()
      } catch (e: IOException) {
        val retryable =
          when (e) {
            is HttpResponseException -> KmsClientWithRetryInfo.isRetryableHttpStatus(e.statusCode)
            is Retryable -> e.isRetryable
            else -> false
          }
        throw KmsKeyAccessException("Failed to refresh Google Cloud credentials", retryable, e)
      }
    }
  }

  internal companion object {
    /** Creates a factory with a supplied authentication transport for tests. */
    @JvmSynthetic
    fun withHttpTransportFactory(
      httpTransportFactory: HttpTransportFactory
    ): GCloudKmsClientFactory = GCloudKmsClientFactory(httpTransportFactory)
  }
}

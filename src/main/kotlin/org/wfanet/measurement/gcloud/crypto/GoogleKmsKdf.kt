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

package org.wfanet.measurement.gcloud.crypto

import com.google.cloud.kms.v1.CryptoKeyVersionName
import com.google.cloud.kms.v1.KeyManagementServiceClient
import com.google.cloud.kms.v1.MacSignRequest
import com.google.cloud.kms.v1.MacSignResponse
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.KeyMaterialGenerator

/**
 * Implementation of [KeyMaterialGenerator] that derives key material by using Google Cloud KMS's
 * MAC (Message Authentication Code) signing operation.
 *
 * This class uses a specified Google Cloud KMS CryptoKeyVersion to generate a cryptographically
 * secure, deterministic key material from the given input.
 *
 * The input to [generateKeyMaterial] is signed using the KMS MAC operation, and the resulting MAC
 * bytes are returned as the derived key material.
 *
 * This approach leverages Google Cloud KMS to securely perform the cryptographic operation,
 * ensuring that the key material generation is backed by a managed, secure key.
 *
 * @property projectId The Google Cloud project ID containing the KMS key.
 * @property locationId The location ID of the KMS key (e.g., "us-central1").
 * @property keyRingId The key ring ID within the specified location.
 * @property cryptoKeyId The ID of the CryptoKey used for the MAC operation.
 * @property cryptoKeyVersionId The specific version of the CryptoKey to use.
 */
class GoogleKmsKdf(
  private val projectId: String,
  private val locationId: String,
  private val keyRingId: String,
  private val cryptoKeyId: String,
  private val cryptoKeyVersionId: String,
) : KeyMaterialGenerator {
  /**
   * Generates key material by signing the input data using Google Cloud KMS MAC operation.
   *
   * @param input The input data to be signed. This input is NOT assumed to be secret.
   * @return A [ByteString] containing the MAC signature bytes, which serve as the derived key
   *   material.
   *
   * This method creates a KMS client, builds a MAC sign request for the specified CryptoKeyVersion,
   * and returns the MAC bytes from the response.
   *
   * Note: This operation requires appropriate permissions and credentials configured via
   * GOOGLE_APPLICATION_CREDENTIALS environment variable or equivalent.
   */
  override fun generateKeyMaterial(input: ByteString): ByteString {
    // Build the full resource name of the CryptoKeyVersion
    val keyVersionName =
      CryptoKeyVersionName.of(projectId, locationId, keyRingId, cryptoKeyId, cryptoKeyVersionId)
    // Create the KMS client (uses GOOGLE_APPLICATION_CREDENTIALS)
    val macSignResponse: MacSignResponse =
      KeyManagementServiceClient.create().use { client: KeyManagementServiceClient ->
        // Build the request
        val request =
          MacSignRequest.newBuilder().setName(keyVersionName.toString()).setData(input).build()
        // Call the API
        client.macSign(request)
      }
    return macSignResponse.mac
  }
}

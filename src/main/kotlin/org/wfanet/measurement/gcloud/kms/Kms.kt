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

import com.google.cloud.kms.v1.CryptoKeyVersionName
import com.google.cloud.kms.v1.KeyManagementServiceClient
import com.google.cloud.kms.v1.MacSignRequest
import com.google.cloud.kms.v1.MacSignResponse
import com.google.protobuf.ByteString

class Kms(
  private val projectId: String,
  private val locationId: String,
  private val keyRingId: String,
  private val cryptoKeyId: String,
  private val cryptoKeyVersionId: String,
) {
  fun hmacSign(data: ByteString): ByteString {
    // Build the full resource name of the CryptoKeyVersion
    val keyVersionName =
      CryptoKeyVersionName.of(projectId, locationId, keyRingId, cryptoKeyId, cryptoKeyVersionId)
    // Create the KMS client (uses GOOGLE_APPLICATION_CREDENTIALS)
    val macSignResponse: MacSignResponse =
      KeyManagementServiceClient.create().use { client: KeyManagementServiceClient ->
        // Build the request
        val request =
          MacSignRequest.newBuilder().setName(keyVersionName.toString()).setData(data).build()
        // Call the API
        client.macSign(request)
      }
    return macSignResponse.mac
  }
}

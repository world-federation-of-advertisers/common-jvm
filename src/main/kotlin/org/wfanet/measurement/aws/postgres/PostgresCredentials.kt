// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.aws.postgres

import com.google.gson.Gson
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse

data class PostgresCredentials(
  val username: String,
  val password: String,
) {
  companion object {
    fun fromAwsSecretManager(region: String, secretName: String): PostgresCredentials {
      val secretManagerCli =
        SecretsManagerClient.builder()
          .region(Region.of(region))
          .credentialsProvider(DefaultCredentialsProvider.create())
          .build()
      val valueRequest = GetSecretValueRequest.builder().secretId(secretName).build()
      val valueResponse: GetSecretValueResponse = secretManagerCli.getSecretValue(valueRequest)
      val secret = valueResponse.secretString()
      return Gson().fromJson(secret, PostgresCredentials::class.java)
    }
  }
}

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
import java.security.GeneralSecurityException
import java.util.Base64
import java.util.Locale
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.KmsClient as SdkKmsClient
import software.amazon.awssdk.services.kms.model.DecryptRequest
import software.amazon.awssdk.services.kms.model.EncryptRequest
import software.amazon.awssdk.services.kms.model.KmsException

/**
 * A Tink [KmsClient] implementation for AWS KMS using AWS SDK v2.
 *
 * This avoids the `tink-awskms` library which depends on AWS SDK v1, allowing usage in projects
 * that standardize on AWS SDK v2.
 *
 * @param credentialsProvider The [AwsCredentialsProvider] to use for authenticating with AWS KMS.
 */
class AwsKmsClient(private val credentialsProvider: AwsCredentialsProvider) : KmsClient {

  override fun doesSupport(keyUri: String?): Boolean {
    return keyUri != null && keyUri.lowercase(Locale.US).startsWith(KEY_URI_PREFIX)
  }

  override fun withCredentials(credentialPath: String?): KmsClient {
    throw UnsupportedOperationException(
      "Use AwsKmsClientFactory to create instances with credentials"
    )
  }

  override fun withDefaultCredentials(): KmsClient {
    throw UnsupportedOperationException(
      "Use AwsKmsClientFactory to create instances with credentials"
    )
  }

  /**
   * Returns an [Aead] backed by the AWS KMS key identified by [keyUri].
   *
   * The region for the KMS client is extracted from the key ARN embedded in the URI.
   *
   * @param keyUri A key URI of the form `aws-kms://arn:aws:kms:REGION:ACCOUNT:key/KEY-ID`.
   * @throws GeneralSecurityException if the URI is unsupported or the KMS client cannot be created.
   */
  override fun getAead(keyUri: String?): Aead {
    if (keyUri == null || !doesSupport(keyUri)) {
      throw GeneralSecurityException("Unsupported key URI: $keyUri")
    }
    val keyArn = keyUri.substring(KEY_URI_PREFIX.length)
    val region = extractRegionFromArn(keyArn)

    val kmsClient =
      try {
        SdkKmsClient.builder()
          .apply {
            credentialsProvider(credentialsProvider)
            region(Region.of(region))
          }
          .build()
      } catch (e: Exception) {
        throw GeneralSecurityException("Cannot initialize AWS KMS client", e)
      }

    return AwsKmsAead(kmsClient, keyArn)
  }

  companion object {
    const val KEY_URI_PREFIX = "aws-kms://"

    private fun extractRegionFromArn(keyArn: String): String {
      // ARN format: arn:aws:kms:REGION:ACCOUNT-ID:key/KEY-ID
      val parts = keyArn.split(":")
      if (parts.size < 4) {
        throw GeneralSecurityException("Invalid AWS KMS key ARN: $keyArn")
      }
      return parts[3]
    }
  }
}

/**
 * An [Aead] implementation backed by AWS KMS using AWS SDK v2.
 *
 * Encryption context with `associatedData` (Base64-encoded) is included when [associatedData] is
 * non-null and non-empty, matching the behavior of Tink's `AwsKmsAead`.
 */
private class AwsKmsAead(private val kmsClient: SdkKmsClient, private val keyArn: String) : Aead {

  override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray {
    try {
      val request =
        EncryptRequest.builder()
          .apply {
            keyId(keyArn)
            plaintext(SdkBytes.fromByteArray(plaintext))
            if (associatedData != null && associatedData.isNotEmpty()) {
              encryptionContext(
                mapOf(ASSOCIATED_DATA_KEY to Base64.getEncoder().encodeToString(associatedData))
              )
            }
          }
          .build()
      val response = kmsClient.encrypt(request)
      return response.ciphertextBlob().asByteArray()
    } catch (e: KmsException) {
      throw GeneralSecurityException("Encryption failed", e)
    }
  }

  override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray {
    try {
      val request =
        DecryptRequest.builder()
          .apply {
            ciphertextBlob(SdkBytes.fromByteArray(ciphertext))
            if (associatedData != null && associatedData.isNotEmpty()) {
              encryptionContext(
                mapOf(ASSOCIATED_DATA_KEY to Base64.getEncoder().encodeToString(associatedData))
              )
            }
          }
          .build()
      val response = kmsClient.decrypt(request)
      if (response.keyId() != keyArn) {
        throw GeneralSecurityException("Decryption failed: wrong key id")
      }
      return response.plaintext().asByteArray()
    } catch (e: KmsException) {
      throw GeneralSecurityException("Decryption failed", e)
    }
  }

  companion object {
    private const val ASSOCIATED_DATA_KEY = "associatedData"
  }
}

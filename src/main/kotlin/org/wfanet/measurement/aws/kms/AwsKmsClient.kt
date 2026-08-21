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
import java.util.concurrent.CompletionException
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kms.KmsClient as SdkKmsClient
import software.amazon.awssdk.services.kms.model.DecryptRequest
import software.amazon.awssdk.services.kms.model.EncryptRequest
import software.amazon.awssdk.services.kms.model.KmsException

/**
 * A Tink [KmsClient] implementation for AWS KMS using AWS SDK v2.
 *
 * @param credentialsProvider Provider of the AWS credentials to authenticate with AWS KMS.
 * @deprecated Superseded by the upstream `com.google.crypto.tink.integration.awskms.AwsKmsClient`
 *   (`tink-awskms` >= 2.0.0), which also targets AWS SDK v2. This class encodes associated data as
 *   Base64, which is NOT compatible with the upstream client's hex encoding when the associated
 *   data is non-empty -- ciphertext produced by one with non-empty associated data cannot be
 *   decrypted by the other (both encode empty associated data identically). New usages should use
 *   the upstream client instead. This class is unreferenced by any factory and kept only as a
 *   fallback in case a currently-undiscovered consumer turns out to still need it to decrypt
 *   previously-written ciphertext; there is no way to make it produce upstream-compatible output.
 *   If no such consumer materializes, it can be deleted outright in a follow-up.
 */
@Deprecated(
  "Superseded by upstream com.google.crypto.tink.integration.awskms.AwsKmsClient (tink-awskms). " +
    "Its Base64 associated-data encoding is not compatible with the upstream client's hex " +
    "encoding for non-empty associated data; it exists only to decrypt ciphertext previously " +
    "written with it."
)
class AwsKmsClient(private val credentialsProvider: IdentityProvider<AwsCredentialsIdentity>) :
  KmsClient {

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
 * When [associatedData] is non-null and non-empty, it is added to the KMS encryption context under
 * the key `associatedData`, Base64-encoded. This is NOT the same encoding upstream Tink's
 * `AwsKmsAead` uses (hex), so ciphertext is not portable between the two.
 */
private class AwsKmsAead(private val kmsClient: SdkKmsClient, private val keyArn: String) : Aead {

  override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray =
    inKmsCall("Encryption") {
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
      kmsClient.encrypt(request).ciphertextBlob().asByteArray()
    }

  override fun decrypt(ciphertext: ByteArray, associatedData: ByteArray?): ByteArray =
    inKmsCall("Decryption") {
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
      response.plaintext().asByteArray()
    }

  /** Runs [block], reporting AWS failures as [GeneralSecurityException] per the [Aead] contract. */
  private inline fun <T> inKmsCall(operation: String, block: () -> T): T =
    try {
      block()
    } catch (e: KmsException) {
      throw GeneralSecurityException("$operation failed", e)
    } catch (e: CompletionException) {
      // The SDK resolves credentials by joining a future, and rethrows the CompletionException
      // as-is when its cause is a checked exception.
      throw GeneralSecurityException("$operation failed", e.cause ?: e)
    }

  companion object {
    private const val ASSOCIATED_DATA_KEY = "associatedData"
  }
}

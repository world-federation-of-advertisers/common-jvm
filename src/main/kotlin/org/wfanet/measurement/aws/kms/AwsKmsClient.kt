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
import software.amazon.awssdk.utils.BinaryUtils

/**
 * How associated data is encoded into the AWS KMS encryption context value.
 *
 * The encryption context is authenticated by AWS KMS, so the encoding used at encrypt time must
 * match the encoding used at decrypt time, including across different client implementations.
 */
enum class AssociatedDataEncoding {
  /**
   * Lowercase hex, byte-for-byte identical to upstream Tink's
   * `com.google.crypto.tink.integration.awskms.AwsKmsAead` (which uses
   * `software.amazon.awssdk.utils.BinaryUtils.toHex`). Use this for interoperability with the
   * upstream `tink-awskms` client.
   */
  HEX,

  /**
   * Base64. This was the original encoding used by this class; it is NOT interoperable with
   * upstream Tink. Retained only to read data written by earlier versions of this client.
   */
  BASE64,
}

/** Encodes [associatedData] for the KMS encryption context using [encoding]. */
internal fun encodeAssociatedData(
  associatedData: ByteArray,
  encoding: AssociatedDataEncoding,
): String =
  when (encoding) {
    AssociatedDataEncoding.HEX -> BinaryUtils.toHex(associatedData)
    AssociatedDataEncoding.BASE64 -> Base64.getEncoder().encodeToString(associatedData)
  }

/**
 * A Tink [KmsClient] implementation for AWS KMS using AWS SDK v2.
 *
 * @param credentialsProvider The [AwsCredentialsProvider] to use for authenticating with AWS KMS.
 * @param associatedDataEncoding How associated data is encoded into the KMS encryption context.
 *   Defaults to [AssociatedDataEncoding.HEX] to match upstream Tink.
 * @deprecated Superseded by the upstream `com.google.crypto.tink.integration.awskms.AwsKmsClient`
 *   (`tink-awskms` >= 2.0.0), which also targets AWS SDK v2. This class remains temporarily until
 *   the Tink version upgrade that pulls in `tink-awskms` lands; construct it with
 *   [AssociatedDataEncoding.HEX] (the default) so its output is interoperable with that client.
 */
@Deprecated(
  "Superseded by upstream com.google.crypto.tink.integration.awskms.AwsKmsClient (tink-awskms). " +
    "Temporary until the Tink upgrade lands; use AssociatedDataEncoding.HEX for interoperability."
)
class AwsKmsClient(
  private val credentialsProvider: AwsCredentialsProvider,
  private val associatedDataEncoding: AssociatedDataEncoding = AssociatedDataEncoding.HEX,
) : KmsClient {

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

    return AwsKmsAead(kmsClient, keyArn, associatedDataEncoding)
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
 * the key `associatedData`, encoded per [associatedDataEncoding]. With [AssociatedDataEncoding.HEX]
 * this matches upstream Tink's `AwsKmsAead`.
 */
internal class AwsKmsAead(
  private val kmsClient: SdkKmsClient,
  private val keyArn: String,
  private val associatedDataEncoding: AssociatedDataEncoding,
) : Aead {

  override fun encrypt(plaintext: ByteArray, associatedData: ByteArray?): ByteArray {
    try {
      val request =
        EncryptRequest.builder()
          .apply {
            keyId(keyArn)
            plaintext(SdkBytes.fromByteArray(plaintext))
            if (associatedData != null && associatedData.isNotEmpty()) {
              encryptionContext(
                mapOf(
                  ASSOCIATED_DATA_KEY to
                    encodeAssociatedData(associatedData, associatedDataEncoding)
                )
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
                mapOf(
                  ASSOCIATED_DATA_KEY to
                    encodeAssociatedData(associatedData, associatedDataEncoding)
                )
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

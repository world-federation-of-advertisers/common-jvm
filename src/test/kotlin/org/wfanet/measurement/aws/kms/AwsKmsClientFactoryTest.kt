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

import com.google.common.truth.Truth.assertThat
import java.security.GeneralSecurityException
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kms.KmsClient as SdkKmsClient
import software.amazon.awssdk.services.kms.model.DecryptRequest
import software.amazon.awssdk.services.kms.model.DecryptResponse
import software.amazon.awssdk.services.kms.model.EncryptRequest
import software.amazon.awssdk.services.kms.model.EncryptResponse
import software.amazon.awssdk.utils.BinaryUtils

private const val AWS_KMS_KEY_URI = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key-id"
private const val KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/test-key-id"
private const val GCP_KMS_KEY_URI = "gcp-kms://projects/test/locations/us/keyRings/kr/cryptoKeys/ck"
private const val INVALID_ARN_KEY_URI = "aws-kms://invalid-arn"

/** Tests for [AwsKmsClient] and [AwsKmsAead]. */
@RunWith(JUnit4::class)
class AwsKmsClientFactoryTest {
  private lateinit var kmsClient: AwsKmsClient

  @Before
  fun setUp() {
    val credentialsProvider =
      StaticCredentialsProvider.create(AwsBasicCredentials.create("fake-key", "fake-secret"))
    kmsClient = AwsKmsClient(credentialsProvider)
  }

  @Test
  fun `doesSupport returns true for aws-kms URIs`() {
    assertThat(kmsClient.doesSupport(AWS_KMS_KEY_URI)).isTrue()
  }

  @Test
  fun `doesSupport returns false for gcp-kms URIs`() {
    assertThat(kmsClient.doesSupport(GCP_KMS_KEY_URI)).isFalse()
  }

  @Test
  fun `doesSupport returns false for null URI`() {
    assertThat(kmsClient.doesSupport(null)).isFalse()
  }

  @Test
  fun `getAead throws GeneralSecurityException for unsupported URI`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(GCP_KMS_KEY_URI) }
  }

  @Test
  fun `getAead throws GeneralSecurityException for invalid ARN`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(INVALID_ARN_KEY_URI) }
  }

  @Test
  fun `encrypt hex-encodes associated data into the encryption context`() {
    val mockKms = mock<SdkKmsClient>()
    whenever(mockKms.encrypt(any<EncryptRequest>()))
      .thenReturn(
        EncryptResponse.builder()
          .ciphertextBlob(SdkBytes.fromByteArray(byteArrayOf(1, 2, 3)))
          .build()
      )
    val aead = AwsKmsAead(mockKms, KEY_ARN)
    // Known vector locks the exact wire encoding (lowercase hex, no separators).
    val associatedData = byteArrayOf(0x00, 0x01, 0x0f, 0x10, 0xab.toByte(), 0xff.toByte())

    aead.encrypt("plaintext".toByteArray(Charsets.UTF_8), associatedData)

    val captor = argumentCaptor<EncryptRequest>()
    verify(mockKms).encrypt(captor.capture())
    assertThat(captor.firstValue.encryptionContext())
      .containsExactly("associatedData", "00010f10abff")
  }

  @Test
  fun `encrypt encryption context matches upstream Tink (BinaryUtils hex)`() {
    val mockKms = mock<SdkKmsClient>()
    whenever(mockKms.encrypt(any<EncryptRequest>()))
      .thenReturn(
        EncryptResponse.builder().ciphertextBlob(SdkBytes.fromByteArray(byteArrayOf(1))).build()
      )
    val aead = AwsKmsAead(mockKms, KEY_ARN)
    val associatedData = "some-blob-key/2026-03-13/data".toByteArray(Charsets.UTF_8)

    aead.encrypt("plaintext".toByteArray(Charsets.UTF_8), associatedData)

    val captor = argumentCaptor<EncryptRequest>()
    verify(mockKms).encrypt(captor.capture())
    assertThat(captor.firstValue.encryptionContext())
      .containsExactly("associatedData", BinaryUtils.toHex(associatedData))
  }

  @Test
  fun `encrypt sends no encryption context when associated data is empty`() {
    val mockKms = mock<SdkKmsClient>()
    whenever(mockKms.encrypt(any<EncryptRequest>()))
      .thenReturn(
        EncryptResponse.builder().ciphertextBlob(SdkBytes.fromByteArray(byteArrayOf(1))).build()
      )
    val aead = AwsKmsAead(mockKms, KEY_ARN)

    aead.encrypt("plaintext".toByteArray(Charsets.UTF_8), ByteArray(0))

    val captor = argumentCaptor<EncryptRequest>()
    verify(mockKms).encrypt(captor.capture())
    assertThat(captor.firstValue.hasEncryptionContext()).isFalse()
  }

  @Test
  fun `decrypt hex-encodes associated data into the encryption context`() {
    val mockKms = mock<SdkKmsClient>()
    val plaintext = "plaintext".toByteArray(Charsets.UTF_8)
    whenever(mockKms.decrypt(any<DecryptRequest>()))
      .thenReturn(
        DecryptResponse.builder()
          .keyId(KEY_ARN)
          .plaintext(SdkBytes.fromByteArray(plaintext))
          .build()
      )
    val aead = AwsKmsAead(mockKms, KEY_ARN)
    val associatedData = "blob-key".toByteArray(Charsets.UTF_8)

    val result = aead.decrypt(byteArrayOf(1, 2, 3), associatedData)

    assertThat(result).isEqualTo(plaintext)
    val captor = argumentCaptor<DecryptRequest>()
    verify(mockKms).decrypt(captor.capture())
    assertThat(captor.firstValue.encryptionContext())
      .containsExactly("associatedData", BinaryUtils.toHex(associatedData))
  }
}

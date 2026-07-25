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

@file:Suppress("DEPRECATION") // Exercises the deprecated AwsKmsClient during its transition period.

package org.wfanet.measurement.aws.kms

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.integration.awskms.AwsKmsClient as TinkAwsKmsClient
import java.security.GeneralSecurityException
import java.util.Base64
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kms.KmsClient as SdkKmsClient
import software.amazon.awssdk.services.kms.model.DecryptRequest
import software.amazon.awssdk.services.kms.model.DecryptResponse
import software.amazon.awssdk.services.kms.model.EncryptRequest
import software.amazon.awssdk.services.kms.model.EncryptResponse
import software.amazon.awssdk.services.kms.model.InvalidCiphertextException

private const val AWS_KMS_KEY_URI = "aws-kms://arn:aws:kms:us-east-1:123456789012:key/test-key-id"
private const val KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/test-key-id"
private const val GCP_KMS_KEY_URI = "gcp-kms://projects/test/locations/us/keyRings/kr/cryptoKeys/ck"
private const val FAKE_KMS_KEY_URI = "fake-kms://key1"
private const val INVALID_ARN_KEY_URI = "aws-kms://invalid-arn"

/** Tests for [AwsKmsClient] and its interoperability with the upstream Tink AWS KMS client. */
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
  fun `doesSupport returns false for fake-kms URIs`() {
    assertThat(kmsClient.doesSupport(FAKE_KMS_KEY_URI)).isFalse()
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
  fun `getAead throws GeneralSecurityException for null URI`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(null) }
  }

  @Test
  fun `getAead throws GeneralSecurityException for invalid ARN`() {
    assertFailsWith<GeneralSecurityException> { kmsClient.getAead(INVALID_ARN_KEY_URI) }
  }

  // Interop: a ciphertext produced by the deprecated custom client with HEX associated-data
  // encoding
  // is decryptable by the upstream Tink AwsKmsClient, but one produced with BASE64 is NOT, because
  // the AWS KMS encryption context differs. Both clients share a fake SDK KmsClient that
  // authenticates the encryption context the way real AWS KMS does.

  @Test
  fun `custom HEX ciphertext is decryptable by upstream Tink AwsKmsClient`() {
    val fakeKms = contextEnforcingFakeKms()
    val plaintext = "dek-material".toByteArray(Charsets.UTF_8)
    val associatedData = "path/2026-03-13/data".toByteArray(Charsets.UTF_8)

    val ciphertext =
      customAead(fakeKms, AssociatedDataEncoding.HEX).encrypt(plaintext, associatedData)
    val upstream = upstreamAeadWith(fakeKms, AWS_KMS_KEY_URI)

    assertThat(upstream.decrypt(ciphertext, associatedData)).isEqualTo(plaintext)
  }

  @Test
  fun `custom BASE64 ciphertext is NOT decryptable by upstream Tink AwsKmsClient`() {
    val fakeKms = contextEnforcingFakeKms()
    val plaintext = "dek-material".toByteArray(Charsets.UTF_8)
    val associatedData = "path/2026-03-13/data".toByteArray(Charsets.UTF_8)

    val ciphertext =
      customAead(fakeKms, AssociatedDataEncoding.BASE64).encrypt(plaintext, associatedData)
    val upstream = upstreamAeadWith(fakeKms, AWS_KMS_KEY_URI)

    assertFailsWith<GeneralSecurityException> { upstream.decrypt(ciphertext, associatedData) }
  }

  /**
   * Builds the file-private custom [AwsKmsAead] for [encoding], backed by [kms], via reflection.
   *
   * [AwsKmsAead] is intentionally private; this mirrors the reflection used below to inject a fake
   * client into the upstream Tink client, so the interop behavior can be tested without widening
   * the production visibility just for tests.
   */
  private fun customAead(kms: SdkKmsClient, encoding: AssociatedDataEncoding): Aead {
    val constructor =
      Class.forName("org.wfanet.measurement.aws.kms.AwsKmsAead")
        .getDeclaredConstructor(
          SdkKmsClient::class.java,
          String::class.java,
          AssociatedDataEncoding::class.java,
        )
    constructor.isAccessible = true
    return constructor.newInstance(kms, KEY_ARN, encoding) as Aead
  }

  /**
   * A fake AWS KMS [SdkKmsClient] that authenticates the encryption context like real AWS KMS: the
   * context supplied at decrypt must exactly match the context supplied at encrypt.
   */
  private fun contextEnforcingFakeKms(): SdkKmsClient {
    val kms = Mockito.mock(SdkKmsClient::class.java)
    Mockito.`when`(kms.encrypt(Mockito.any(EncryptRequest::class.java))).thenAnswer { invocation ->
      val request = invocation.getArgument(0, EncryptRequest::class.java)
      val context = request.encryptionContext()["associatedData"] ?: ""
      val plaintext = request.plaintext().asByteArray()
      val blob = context + "|" + Base64.getEncoder().encodeToString(plaintext)
      EncryptResponse.builder().keyId(KEY_ARN).ciphertextBlob(SdkBytes.fromUtf8String(blob)).build()
    }
    Mockito.`when`(kms.decrypt(Mockito.any(DecryptRequest::class.java))).thenAnswer { invocation ->
      val request = invocation.getArgument(0, DecryptRequest::class.java)
      val context = request.encryptionContext()["associatedData"] ?: ""
      val parts = request.ciphertextBlob().asUtf8String().split("|", limit = 2)
      if (parts.size != 2 || parts[0] != context) {
        throw InvalidCiphertextException.builder().message("encryption context mismatch").build()
      }
      DecryptResponse.builder()
        .keyId(KEY_ARN)
        .plaintext(SdkBytes.fromByteArray(Base64.getDecoder().decode(parts[1])))
        .build()
    }
    return kms
  }

  /** Builds an upstream Tink [Aead] backed by [fakeKms] (injected via the package-private hook). */
  private fun upstreamAeadWith(fakeKms: SdkKmsClient, keyUri: String): Aead {
    val client = TinkAwsKmsClient()
    val withAwsKms =
      TinkAwsKmsClient::class.java.getDeclaredMethod("withAwsKms", SdkKmsClient::class.java)
    withAwsKms.isAccessible = true
    withAwsKms.invoke(client, fakeKms)
    return client.getAead(keyUri)
  }
}

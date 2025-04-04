/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class WithEnvelopeEncryptionTest() {

  @Test
  fun `returns encrypted streaming AEAD storage client`() {
    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val wrappedStorageClient = InMemoryStorageClient()

    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)
  }

  @Test
  fun `unsupported key type throws IllegalArgumentException`() {
    runBlocking {
      val kmsClient = FakeKmsClient()
      val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      // Only streaming is supported for the key template type
      val tinkKeyTemplateType = "AES128_GCM"
      val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
      val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
      val wrappedStorageClient = InMemoryStorageClient()

      val serializedEncryptionKey =
        ByteString.copyFrom(
          TinkProtoKeysetFormat.serializeEncryptedKeyset(
            keyEncryptionHandle,
            kmsClient.getAead(kekUri),
            byteArrayOf(),
          )
        )
      assertFailsWith<IllegalArgumentException> {
        wrappedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
      }
    }
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  companion object {
    private const val TINK_PREFIX_SIZE_BYTES = 5
    private const val HEADER_SIZE_BYTES = 1
    private const val SEGMENT_INFO_SIZE_BYTES = 21
    private const val FIRST_SEGMENT_HEADER_SIZE = 21
    private const val SEGMENT_TAG_SIZE_BYTES = 16
    private const val LAST_SEGMENT_HEADER_SIZE = 24

    private const val TOTAL_OVERHEAD_BYTES =
      TINK_PREFIX_SIZE_BYTES +
        HEADER_SIZE_BYTES +
        SEGMENT_INFO_SIZE_BYTES +
        FIRST_SEGMENT_HEADER_SIZE +
        SEGMENT_TAG_SIZE_BYTES +
        LAST_SEGMENT_HEADER_SIZE
  }
}

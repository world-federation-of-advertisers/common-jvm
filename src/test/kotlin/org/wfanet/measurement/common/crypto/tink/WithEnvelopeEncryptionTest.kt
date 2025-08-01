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
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.testing.KeyDerivation
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class WithEnvelopeEncryptionTest() {

  @Test
  fun `returns encrypted streaming AEAD storage client for proper encryptedDek`() {
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
      wrappedStorageClient.withEnvelopeEncryption(
        kmsClient,
        kekUri,
        encryptedDek = serializedEncryptionKey,
      )
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)
  }

  @Test
  fun `unsupported key type for encryptedDek throws IllegalArgumentException`() {
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
        wrappedStorageClient.withEnvelopeEncryption(
          kmsClient,
          kekUri,
          encryptedDek = serializedEncryptionKey,
        )
      }
    }
  }

  @Test
  fun `able to write and read storage client with identical kdfs`() {
    val wrappedStorageClient = InMemoryStorageClient()

    val kdf =
      KeyDerivation(
        ikm = "something-super-secret".toByteStringUtf8(),
        salt = "some-optional-salt".toByteStringUtf8(),
      )

    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf,
      )
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)
    runBlocking { storageClient.writeBlob("some-blob-key", "content".toByteStringUtf8()) }
    val kdf2 =
      KeyDerivation(
        ikm = "something-super-secret".toByteStringUtf8(),
        salt = "some-optional-salt".toByteStringUtf8(),
      )
    val storageClient2 =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf2,
      )
    val content: ByteString = runBlocking {
      storageClient2.getBlob("some-blob-key")!!.read().flatten()
    }
    assertThat(content).isEqualTo("content".toByteStringUtf8())
  }

  @Test
  fun `content written to store withEnvelopeEncryption using kdf is different`() {
    val wrappedStorageClient = InMemoryStorageClient()

    val kdf =
      KeyDerivation(
        ikm = "something-super-secret".toByteStringUtf8(),
        salt = "some-optional-salt".toByteStringUtf8(),
      )
    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf,
      )
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)
    runBlocking { storageClient.writeBlob("some-blob-key", "content".toByteStringUtf8()) }
    val content: ByteString = runBlocking {
      wrappedStorageClient.getBlob("some-blob-key")!!.read().flatten()
    }
    assertThat(content).isNotEqualTo("content".toByteStringUtf8())
  }

  @Test
  fun `fails to read with different kdf`() {
    val wrappedStorageClient = InMemoryStorageClient()

    val kdf =
      KeyDerivation(
        ikm = "something-super-secret".toByteStringUtf8(),
        salt = "some-optional-salt".toByteStringUtf8(),
      )
    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf,
      )
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)
    runBlocking { storageClient.writeBlob("some-blob-key", "content".toByteStringUtf8()) }
    val kdf2 =
      KeyDerivation(
        ikm = "some-other-secret".toByteStringUtf8(),
        salt = "some-optional-salt".toByteStringUtf8(),
      )
    val storageClient2 =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf2,
      )
    assertFails { runBlocking { storageClient2.getBlob("some-blob-key")!!.read().flatten() } }
  }

  @Test
  fun `fails with invalid shared secret but correct kdf`() {
    val wrappedStorageClient = InMemoryStorageClient()

    val kdf =
      KeyDerivation(
        ikm = "something-super-secret".toByteStringUtf8(),
        salt = "some-optional-salt".toByteStringUtf8(),
      )
    val storageClient =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf,
      )
    assertThat(storageClient).isInstanceOf(StreamingAeadStorageClient::class.java)
    runBlocking { storageClient.writeBlob("some-blob-key", "content".toByteStringUtf8()) }
    val storageClient2 =
      wrappedStorageClient.withEnvelopeEncryption(
        kdfSharedSecret = "some-other-shared-secret".toByteStringUtf8(),
        keyMaterialGenerator = kdf,
      )
    assertFails { runBlocking { storageClient2.getBlob("some-blob-key")!!.read().flatten() } }
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }
}

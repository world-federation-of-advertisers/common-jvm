/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.storage

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import kotlin.test.assertFailsWith
import org.apache.hadoop.conf.Configuration
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient

@RunWith(JUnit4::class)
class ParquetKmsClientTest {
  private fun newAead(): Aead =
    KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM")).getPrimitive(Aead::class.java)

  private fun fakeKms(vararg kekUris: String): FakeKmsClient =
    FakeKmsClient().also { client -> kekUris.forEach { client.setAead(it, newAead()) } }

  private fun bridgeFor(config: ParquetEncryptionConfig): ParquetKmsClient {
    val conf = Configuration()
    ParquetKmsClient.register(conf, config)
    return ParquetKmsClient().apply { initialize(conf, null, null, null) }
  }

  @Test
  fun `register sets the crypto factory, kms client class, and instance id`() {
    val conf = Configuration()
    ParquetKmsClient.register(conf, ParquetEncryptionConfig(kmsProvider = { FakeKmsClient() }))

    assertThat(conf.get("parquet.crypto.factory.class"))
      .isEqualTo("org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
    assertThat(conf.get("parquet.encryption.kms.client.class"))
      .isEqualTo(ParquetKmsClient::class.java.name)
    val id = conf.get(ParquetKmsClient.PROVIDER_KEY)
    assertThat(id).isNotEmpty()
    // instance id == provider id keeps parquet's KeyToolkit cache per-instance.
    assertThat(conf.get("parquet.encryption.kms.instance.id")).isEqualTo(id)
  }

  @Test
  fun `wrapKey then unwrapKey round-trips through the Tink Aead`() {
    val kekUri = "fake-kms://kek"
    val bridge = bridgeFor(ParquetEncryptionConfig(kmsProvider = { fakeKms(kekUri) }))

    val key = ByteArray(32) { it.toByte() }
    assertThat(bridge.unwrapKey(bridge.wrapKey(key, kekUri), kekUri)).isEqualTo(key)
  }

  @Test
  fun `keyMapping resolves a short master-key name to a Tink URI`() {
    val kekUri = "fake-kms://kek"
    val bridge =
      bridgeFor(
        ParquetEncryptionConfig(
          kmsProvider = { fakeKms(kekUri) },
          keyMapping = mapOf("kek" to kekUri),
        )
      )

    val key = ByteArray(16) { (it + 1).toByte() }
    assertThat(bridge.unwrapKey(bridge.wrapKey(key, "kek"), "kek")).isEqualTo(key)
  }

  @Test
  fun `wrapKey fails for an unmapped master key`() {
    val bridge = bridgeFor(ParquetEncryptionConfig(kmsProvider = { fakeKms("fake-kms://kek") }))

    assertFailsWith<IllegalArgumentException> { bridge.wrapKey(ByteArray(16), "missing") }
  }

  @Test
  fun `initialize fails when the provider id is not registered`() {
    val conf = Configuration().apply { set(ParquetKmsClient.PROVIDER_KEY, "nope") }

    assertFailsWith<IllegalStateException> { ParquetKmsClient().initialize(conf, null, null, null) }
  }

  @Test
  fun `registrations are isolated per id`() {
    val bridgeA = bridgeFor(ParquetEncryptionConfig(kmsProvider = { fakeKms("fake-kms://a") }))
    val bridgeB = bridgeFor(ParquetEncryptionConfig(kmsProvider = { fakeKms("fake-kms://b") }))
    val key = ByteArray(32) { it.toByte() }

    assertThat(bridgeA.unwrapKey(bridgeA.wrapKey(key, "fake-kms://a"), "fake-kms://a"))
      .isEqualTo(key)
    assertFailsWith<IllegalArgumentException> { bridgeA.wrapKey(key, "fake-kms://b") }
    assertThat(bridgeB.unwrapKey(bridgeB.wrapKey(key, "fake-kms://b"), "fake-kms://b"))
      .isEqualTo(key)
  }

  companion object {
    init {
      // Static one-time Tink Aead registration shared by every test.
      AeadConfig.register()
    }
  }
}

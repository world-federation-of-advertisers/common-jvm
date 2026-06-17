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

import com.google.crypto.tink.KmsClient as TinkKmsClient
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.keytools.KmsClient

/**
 * Bridges a WFA Tink KMS client (`com.google.crypto.tink.KmsClient`) to parquet-mr's key-tools KMS
 * interface (`org.apache.parquet.crypto.keytools.KmsClient`).
 *
 * parquet-mr instantiates this reflectively (no-arg ctor) and calls [initialize] once per
 * reader/writer; it then drives all DEK/KEK wrapping through [wrapKey] / [unwrapKey], which we
 * forward to a Tink `Aead`. The [ParquetEncryptionConfig] for a given client is looked up via a
 * per-instance registration keyed by a UUID stored in the [Configuration] — no global mutable
 * singletons.
 */
class ParquetKmsClient : KmsClient {
  private lateinit var kmsClient: TinkKmsClient
  private lateinit var keyMapping: Map<String, String>

  override fun initialize(
    configuration: Configuration,
    kmsInstanceID: String?,
    kmsInstanceURL: String?,
    accessToken: String?,
  ) {
    val id = configuration.get(PROVIDER_KEY)
    val registration = registrations[id] ?: error("No KMS provider registered for id '$id'")
    kmsClient = registration.kmsProvider()
    keyMapping = registration.keyMapping
  }

  override fun wrapKey(keyBytes: ByteArray, masterKeyIdentifier: String): String {
    val aead = kmsClient.getAead(resolveUri(masterKeyIdentifier))
    return Base64.getEncoder().encodeToString(aead.encrypt(keyBytes, EMPTY_AAD))
  }

  override fun unwrapKey(wrappedKey: String, masterKeyIdentifier: String): ByteArray {
    val aead = kmsClient.getAead(resolveUri(masterKeyIdentifier))
    return aead.decrypt(Base64.getDecoder().decode(wrappedKey), EMPTY_AAD)
  }

  /** Full Tink URIs pass through; short logical names resolve via [keyMapping]. */
  private fun resolveUri(masterKeyId: String): String {
    val uri =
      if (kmsClient.doesSupport(masterKeyId)) masterKeyId
      else
        requireNotNull(keyMapping[masterKeyId]) {
          "No KMS URI for key '$masterKeyId'. Add it to ParquetEncryptionConfig.keyMapping."
        }
    logger.fine { "Resolved master key id '$masterKeyId' to KMS URI '$uri'" }
    return uri
  }

  companion object {
    /** Conf key holding the per-instance registration UUID. */
    const val PROVIDER_KEY = "wfa.parquet.kms.provider.id"

    private val logger = Logger.getLogger(ParquetKmsClient::class.java.name)

    private val EMPTY_AAD = ByteArray(0)

    private data class Registration(
      val kmsProvider: () -> TinkKmsClient,
      val keyMapping: Map<String, String>,
    )

    private val registrations = ConcurrentHashMap<String, Registration>()

    /**
     * Registers [config] against [conf] and points parquet-mr's crypto factory + KMS-client class
     * at this bridge, so reads and writes through [conf] use it.
     *
     * Mutates [conf]: sets [PROVIDER_KEY] (plus the parquet crypto-factory, kms-client-class, and
     * kms-instance-id keys) to a per-registration UUID. Each `ParquetStorageClient` MUST therefore
     * use its **own** [Configuration] instance — registering a second client on the same [conf]
     * overwrites the first's provider id, after which both resolve to the last registration.
     */
    fun register(conf: Configuration, config: ParquetEncryptionConfig) {
      val id = UUID.randomUUID().toString()
      registrations[id] = Registration(config.kmsProvider, config.keyMapping)
      conf.set(PROVIDER_KEY, id)
      conf.set(
        "parquet.crypto.factory.class",
        "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
      )
      conf.set("parquet.encryption.kms.client.class", ParquetKmsClient::class.java.name)
      // parquet-mr's KeyToolkit caches the KMS client per (kms.instance.id,
      // access.token), both defaulting to "DEFAULT". Use the per-registration
      // UUID as the instance id so each client gets its own bridge (initialized
      // with its own conf) instead of colliding on the default cache key with
      // other ParquetStorageClient instances in the same JVM.
      conf.set("parquet.encryption.kms.instance.id", id)
    }
  }
}

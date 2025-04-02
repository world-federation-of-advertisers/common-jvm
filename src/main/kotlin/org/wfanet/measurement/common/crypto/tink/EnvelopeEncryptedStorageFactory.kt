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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.util.Base64
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.storage.StorageClient

/*
 * Used to decrypt data given an encrypted data encryption key.
 * 1. Decrypts the data encryption key using a kms
 * 2. Uses that data encryption key to construct an decrypting/encrypting storage client
 * Currently only supported Streaming AEAD storage client.
 * @param storageClient the underlying [StorageClient]
 * @param kmsClient the Tink [KmsClient] that is used
 * @param config the config for the encryption
 */
class EnvelopeEncryptedStorageFactory(
  private val storageClient: StorageClient,
  private val kmsClient: KmsClient,
  private val config: StorageConfig,
  private val aeadContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) {

  /*
   * Encryption information needed to read the storage
   */
  data class StorageConfig(val kekUri: String, val encryptedDek: String)

  private val kekAead = kmsClient.getAead(config.kekUri)
  private val handle: KeysetHandle = runBlocking {
    withContext(aeadContext) {
      TinkProtoKeysetFormat.parseEncryptedKeyset(
        Base64.getDecoder().decode(config.encryptedDek),
        kekAead,
        byteArrayOf(),
      )
    }
  }

  /*
   * @return the [StorageClient] to read the encrypted data
   */
  fun build(): StorageClient {
    val keyType =
      handle.keysetInfo.keyInfoList
        .filter { it.keyId == handle.keysetInfo.primaryKeyId }
        .single()
        .typeUrl
    return when (keyType) {
      "type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey" -> {

        StreamingAeadConfig.register()
        StreamingAeadStorageClient(
          storageClient = storageClient,
          streamingAead = handle.getPrimitive(StreamingAead::class.java),
          streamingAeadContext = aeadContext,
        )
      }
      else -> throw IllegalArgumentException("Unsupported Key Type: $keyType")
    }
  }
}

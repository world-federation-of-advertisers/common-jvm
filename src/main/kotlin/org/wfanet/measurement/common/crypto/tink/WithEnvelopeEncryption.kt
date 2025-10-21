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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.Key
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.aead.AeadKey
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadKey
import com.google.protobuf.ByteString
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.storage.StorageClient

/*
 * Wraps this [StorageClient] in one that provides envelope encryption.
 * Currently only supported Streaming AEAD storage client.
 * @param kmsClient the Tink [KmsClient] that is used
 * @param kekUri the uri of the key encryption key (kek)
 * @param encryptedDek the encrypted data encryption key (DEK). By default this is
 *        expected to be in Tink binary format, an alternative format (e.g. JSON)
 *        can be supported by providing a custom [parseEncryptedKeyset].
 * @aeadContext the context the encrypted storage client will use. Only needed for streamining use cases.
 * @param parseEncryptedKeyset function used to parse and decrypt the [encryptedDek] into a [KeysetHandle].
 *        The default is [TinkProtoKeysetFormat.parseEncryptedKeyset], which assumes a Tink-encrypted binary keyset.
 */
fun StorageClient.withEnvelopeEncryption(
  kmsClient: KmsClient,
  kekUri: String,
  encryptedDek: ByteString,
  aeadContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  parseEncryptedKeyset:
    (encryptedDek: ByteArray, kekAead: Aead, associatedData: ByteArray?) -> KeysetHandle =
    TinkProtoKeysetFormat::parseEncryptedKeyset,
): StorageClient {

  AeadConfig.register()
  StreamingAeadConfig.register()

  val storageClient = this
  val kekAead = kmsClient.getAead(kekUri)
//  val handle: KeysetHandle = parseEncryptedKeyset(encryptedDek.toByteArray(), kekAead, null)
    val handle = try {
        parseEncryptedKeyset(encryptedDek.toByteArray(), kekAead, null)
    } catch (e: Exception) {
        println("❌ [Envelope] Failed to unwrap DEK: ${e.message}")
        throw e
    }
    println("✅ [Envelope] Successfully unwrapped DEK. Primary key type: ${handle.primary.key.javaClass.simpleName}")
  return when (val primaryKey: Key = handle.primary.key) {
    is StreamingAeadKey -> {
      println("STREAMING")
      StreamingAeadStorageClient(
        storageClient = storageClient,
        streamingAead = handle.getPrimitive(StreamingAead::class.java),
        streamingAeadContext = aeadContext,
      )
    }
    is AeadKey -> {
      println("AEAD")
      AeadStorageClient(storageClient = storageClient, aead = handle.getPrimitive(Aead::class.java))
    }
    else -> throw IllegalArgumentException("Unsupported Key Type: ${primaryKey::class.simpleName}")
  }
}

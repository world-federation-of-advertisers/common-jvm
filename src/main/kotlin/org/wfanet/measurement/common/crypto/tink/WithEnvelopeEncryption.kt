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
import com.google.crypto.tink.aead.AeadConfig
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
 * @param encrypted data encryption key (DEK) in Tink binary format
 * @aeadContext the context the encrypted storage client will use
 */
fun StorageClient.withEnvelopeEncryption(
  kmsClient: KmsClient,
  kekUri: String,
  kdfSharedSecret: ByteString?,
  encryptedDek: ByteString?,
  macSign: ((data: ByteString) -> ByteString)?,
  aeadContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
): StorageClient {

  AeadConfig.register()
  StreamingAeadConfig.register()

  val storageClient = this
  val handle: KeysetHandle =
    if (encryptedDek != null) {
      val kekAead = kmsClient.getAead(kekUri)
      val handle: KeysetHandle =
        TinkProtoKeysetFormat.parseEncryptedKeyset(
          encryptedDek.toByteArray(),
          kekAead,
          byteArrayOf(),
        )
      require(handle.primary.key is StreamingAeadKey) {
        "Unsupported Key Type: ${handle.primary.key::class.simpleName}"
      }
      handle
    } else {
      check(kdfSharedSecret != null) { "EncryptedDek and kdfSharedSecret cannot both be null" }
      require(macSign != null) { "generateMac must be set if kdfSharedSecret is set" }
      val macResponse = macSign(kdfSharedSecret)
      KeyDerivation.deriveStreamingAeadKeysetHandleWithHKDF(macResponse, null, kdfSharedSecret)
    }
  val streamingAead = handle.getPrimitive(StreamingAead::class.java)
  return StreamingAeadStorageClient(
    storageClient = storageClient,
    streamingAead = streamingAead,
    streamingAeadContext = aeadContext,
  )
}

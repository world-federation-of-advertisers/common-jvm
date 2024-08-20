// Copyright 2021 The Cross-Media Measurement Authors
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
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.ByteString
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.crypto.KeyBlobStore
import org.wfanet.measurement.common.crypto.PrivateKeyStore as CryptoPrivateKeyStore
import org.wfanet.measurement.common.flatten

internal class PrivateKeyStore(
  private val store: KeyBlobStore,
  private val aead: Aead,
  private val aeadContext: @BlockingExecutor CoroutineContext
) : CryptoPrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle> {

  override suspend fun read(keyId: TinkKeyId): TinkPrivateKeyHandle? {
    val privateKeyBlob = store.get(keyId) ?: return null
    val privateKeyBlobFlat = privateKeyBlob.read().flatten().toByteArray()
    val keysetHandle = withContext(aeadContext) {
      TinkProtoKeysetFormat.parseEncryptedKeyset(privateKeyBlobFlat, aead, byteArrayOf())
    }
    return TinkPrivateKeyHandle(keysetHandle)
  }

  override suspend fun write(privateKey: TinkPrivateKeyHandle): String {
    val serializedKeyset = withContext(aeadContext) {
      TinkProtoKeysetFormat.serializeEncryptedKeyset(privateKey.keysetHandle, aead, byteArrayOf())
    }
    val privateKeyBlob = store.write(TinkKeyId(privateKey.publicKey), ByteString.copyFrom(serializedKeyset))
    return privateKeyBlob.blobKey
  }
}

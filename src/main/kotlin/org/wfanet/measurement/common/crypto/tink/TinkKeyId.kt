// Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.common.hash.Hashing
import com.google.crypto.tink.KeysetWriter
import com.google.crypto.tink.proto.EncryptedKeyset
import com.google.crypto.tink.proto.Keyset
import kotlin.properties.Delegates
import org.wfanet.measurement.common.crypto.PrivateKeyStore

data class TinkKeyId(private val primaryKeyId: Long) : PrivateKeyStore.KeyId {
  constructor(
    publicKey: TinkPublicKeyHandle
  ) : this(KeyIdWriter().also { publicKey.keysetHandle.writeNoSecret(it) }.keyId)

  override val blobKey: String
    get() = primaryKeyId.toString()
}

private class KeyIdWriter : KeysetWriter {
  var keyId by Delegates.notNull<Long>()
    private set

  override fun write(keyset: Keyset) {
    val primaryKey =
      requireNotNull(keyset.keyList.find { it.keyId == keyset.primaryKeyId }) {
        "Primary key cannot be found."
      }
    val data = primaryKey.keyData.value
    keyId = Hashing.farmHashFingerprint64().hashBytes(data.toByteArray()).asLong()
  }

  override fun write(keyset: EncryptedKeyset) {
    throw UnsupportedOperationException("write EncryptedKeyset should not be called.")
  }
}

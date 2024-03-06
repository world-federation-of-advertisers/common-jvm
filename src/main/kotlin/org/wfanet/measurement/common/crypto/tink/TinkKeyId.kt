// Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.crypto.tink.KeysetWriter
import com.google.crypto.tink.proto.EncryptedKeyset
import com.google.crypto.tink.proto.Keyset
import com.google.protobuf.ByteString
import kotlin.properties.Delegates
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.toLong

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

  override fun write(keyset: Keyset?) {
    requireNotNull(keyset) { "Keyset cannot be null." }
    val primaryKey =
      keyset.keyList.find { it.keyId == keyset.primaryKeyId }
        ?: error("Primary key cannot be found.")
    val data =
      ByteString.newOutput().use {
        it.write(primaryKey.keyData.value.toByteArray())
        it.toByteString()
      }
    keyId = Hashing.hashSha256(data).substring(0, 8).toLong()
  }

  override fun write(keyset: EncryptedKeyset?) {
    // Supposed not to be used.
  }
}

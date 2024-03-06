/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeysetWriter
import com.google.crypto.tink.proto.EncryptedKeyset
import com.google.crypto.tink.proto.Keyset
import com.google.protobuf.ByteString
import java.io.OutputStream
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.toLong

data class TinkKeyId(private val primaryKeyId: Long) : PrivateKeyStore.KeyId {
  constructor(publicKey: TinkPublicKeyHandle) : this(getTinkKeyId(publicKey))

  override val blobKey: String
    get() = primaryKeyId.toString()
}

private fun getTinkKeyId(publicKey: TinkPublicKeyHandle): Long {
  val publicKeyData =
    ByteString.newOutput().use {
      publicKey.keysetHandle.write(CleartextKeysetWriter(it), CleartextAead())
      it.toByteString()
    }
  return Hashing.hashSha256(publicKeyData).substring(0, 8).toLong()
}

internal class CleartextKeysetWriter(private val stream: OutputStream) : KeysetWriter {
  override fun write(keyset: Keyset?) {
    requireNotNull(keyset) { "Keyset cannot be null." }
    val primaryKey =
      keyset.keyList.find { it.keyId == keyset.primaryKeyId }
        ?: error("Primary key cannot be found.")
    stream.use { it.write(primaryKey.keyData.value.toByteArray()) }
  }

  override fun write(keyset: EncryptedKeyset?) {
    requireNotNull(keyset) { "Keyset cannot be null." }
    stream.use { it.write(keyset.encryptedKeyset.toByteArray()) }
  }
}

internal class CleartextAead : Aead {
  override fun encrypt(plaintext: ByteArray?, associatedData: ByteArray?): ByteArray {
    requireNotNull(plaintext) { "plaintext cannot be null." }
    return plaintext
  }

  override fun decrypt(ciphertext: ByteArray?, associatedData: ByteArray?): ByteArray {
    requireNotNull(ciphertext) { "plaintext cannot be null." }
    return ciphertext
  }
}

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

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.Key
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import com.google.crypto.tink.proto.AesGcmHkdfStreamingParams
import com.google.crypto.tink.proto.AesGcmHkdfStreamingKey
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import java.security.SecureRandom
import kotlin.math.absoluteValue

fun parseJsonEncryptedKey(
    kmsClient: KmsClient,
    kekUri: String,
    encryptedDek: ByteString
): KeysetHandle {
    val kmsAead = kmsClient.getAead(kekUri)
    val decrypted = kmsAead.decrypt(encryptedDek.toByteArray(), byteArrayOf())

    val builder = EncryptionKey.newBuilder()
    JsonFormat.parser().ignoringUnknownFields()
        .merge(decrypted.toString(Charsets.UTF_8), builder)

    val encryptionKey = builder.build()

    val aesKey = when (encryptionKey.keyCase) {
        EncryptionKey.KeyCase.AES_GCM_HKDF_STREAMING_KEY ->
            encryptionKey.aesGcmHkdfStreamingKey
        EncryptionKey.KeyCase.KEY_NOT_SET ->
            throw IllegalArgumentException("EncryptionKey has no key_type set")
    }

    val params = AesGcmHkdfStreamingParams.newBuilder()
        .setDerivedKeySize(aesKey.params.derivedKeySize)
        .setHkdfHashType(aesKey.params.hkdfHashType.mapHashTypeCloneToTink())
        .setCiphertextSegmentSize(aesKey.params.ciphertextSegmentSize)
        .build()

    val tinkKey = AesGcmHkdfStreamingKey.newBuilder()
        .setParams(params)
        .setKeyValue(aesKey.keyValue)
        .setVersion(aesKey.version)
        .build()

    val keyData = KeyData.newBuilder()
        .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
        .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
        .setValue(tinkKey.toByteString())
        .build()

    val keyId = SecureRandom().nextInt().absoluteValue
    val ks = Keyset.newBuilder()
        .setPrimaryKeyId(keyId)
        .addKey(
            Keyset.Key.newBuilder()
                .setKeyId(keyId)
                .setStatus(KeyStatusType.ENABLED)
                .setOutputPrefixType(OutputPrefixType.RAW)
                .setKeyData(keyData)
        )
        .build()

    return CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(ks.toByteArray()))
}
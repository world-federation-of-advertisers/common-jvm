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

import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.proto.AesGcmHkdfStreamingKey
import com.google.crypto.tink.proto.AesGcmHkdfStreamingParams
import com.google.crypto.tink.proto.AesGcmKey
import com.google.crypto.tink.proto.HashType
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import com.google.crypto.tink.subtle.Hkdf
import com.google.protobuf.ByteString
import java.security.SecureRandom

object KeyDerivation {
  fun deriveKeysetHandleWithHKDF(
    ikm: ByteString, // input keying material (e.g. from KMS or MAC)
    salt: ByteString?,
    info: ByteString,
    keySize: Int = 32, // 32 bytes for AES-256-GCM
  ): KeysetHandle {
    // 1. Derive key material using HKDF
    val derivedKey =
      Hkdf.computeHkdf(
        "HmacSha256",
        ikm.toByteArray(),
        salt?.toByteArray(),
        info.toByteArray(),
        keySize,
      )

    // 2. Build an AesGcmKey proto
    val aesGcmKey =
      AesGcmKey.newBuilder().setKeyValue(ByteString.copyFrom(derivedKey)).setVersion(0).build()

    // 3. Build a KeyData proto
    val keyData =
      KeyData.newBuilder()
        .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmKey")
        .setValue(aesGcmKey.toByteString())
        .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
        .build()

    // 4. Build a Keyset proto
    val keyId = SecureRandom().nextInt() and 0x7fffffff // positive int
    val key =
      Keyset.Key.newBuilder()
        .setKeyId(keyId)
        .setStatus(KeyStatusType.ENABLED)
        .setOutputPrefixType(OutputPrefixType.TINK)
        .setKeyData(keyData)
        .build()

    val keyset = Keyset.newBuilder().addKey(key).setPrimaryKeyId(keyId).build()

    // 5. Wrap in a KeysetHandle
    return CleartextKeysetHandle.fromKeyset(keyset)
  }

  fun deriveStreamingAeadKeysetHandleWithHKDF(
    ikm: ByteString, // input keying material (e.g., MAC or shared secret)
    salt: ByteString?,
    info: ByteString,
    keySize: Int = 32, // 32 bytes for AES-256
  ): KeysetHandle {
    // 1. Derive key material using HKDF
    val derivedKey =
      Hkdf.computeHkdf(
        "HmacSha256",
        ikm.toByteArray(),
        salt?.toByteArray(),
        info.toByteArray(),
        keySize,
      )
    // 2. Build AesGcmHkdfStreamingKey proto
    val streamingParams =
      AesGcmHkdfStreamingParams.newBuilder()
        .setCiphertextSegmentSize(4096)
        .setDerivedKeySize(32)
        .setHkdfHashType(HashType.SHA256)
        .build()
    val streamingKey =
      AesGcmHkdfStreamingKey.newBuilder()
        .setVersion(0)
        .setKeyValue(ByteString.copyFrom(derivedKey))
        .setParams(streamingParams)
        .build()
    // 3. Build KeyData proto
    val keyData =
      KeyData.newBuilder()
        .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
        .setValue(streamingKey.toByteString())
        .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
        .build()
    // 4. Build Keyset proto
    val keyId = SecureRandom().nextInt() and 0x7fffffff
    val key =
      Keyset.Key.newBuilder()
        .setKeyId(keyId)
        .setStatus(KeyStatusType.ENABLED)
        .setOutputPrefixType(OutputPrefixType.RAW)
        .setKeyData(keyData)
        .build()
    val keyset = Keyset.newBuilder().addKey(key).setPrimaryKeyId(keyId).build()
    // 5. Wrap in a KeysetHandle
    return CleartextKeysetHandle.fromKeyset(keyset)
  }
}

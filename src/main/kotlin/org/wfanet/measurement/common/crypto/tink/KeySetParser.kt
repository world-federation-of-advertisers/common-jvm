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
import com.google.crypto.tink.proto.HashType
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import com.google.protobuf.ByteString
import java.security.SecureRandom

object KeySetParser {
  fun toStreamingAesKey(derivedKey: ByteString): KeysetHandle {
    // 1. Build AesGcmHkdfStreamingKey proto
    val streamingParams =
      AesGcmHkdfStreamingParams.newBuilder()
        .setCiphertextSegmentSize(4096)
        .setDerivedKeySize(32)
        .setHkdfHashType(HashType.SHA256)
        .build()
    val streamingKey =
      AesGcmHkdfStreamingKey.newBuilder()
        .setVersion(0)
        .setKeyValue(derivedKey)
        .setParams(streamingParams)
        .build()
    // 2. Build KeyData proto
    val keyData =
      KeyData.newBuilder()
        .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
        .setValue(streamingKey.toByteString())
        .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
        .build()
    // 3. Build Keyset proto
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
    return CleartextKeysetHandle.fromKeyset(keyset)!!
  }
}

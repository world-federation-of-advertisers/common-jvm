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

import com.google.crypto.tink.KeysetHandle
import org.wfanet.measurement.common.crypto.PrivateKeyHandle as CryptoPrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle as CryptoPublicKeyHandle

internal open class KeyHandle(val keyId: String, val keysetHandle: KeysetHandle)

internal class PublicKeyHandle(keyId: String, keysetHandle: KeysetHandle) :
  CryptoPublicKeyHandle, KeyHandle(keyId, keysetHandle) {
  init {
    require(!keysetHandle.primaryKey().hasSecret())
  }
}

internal class PrivateKeyHandle(keyId: String, keysetHandle: KeysetHandle) :
  CryptoPrivateKeyHandle, KeyHandle(keyId, keysetHandle) {
  init {
    require(keysetHandle.primaryKey().hasSecret())
  }

  override val publicKey = PublicKeyHandle(keyId, keysetHandle.publicKeysetHandle)
}

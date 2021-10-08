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

import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.HybridCryptor
import org.wfanet.measurement.common.crypto.PrivateKeyHandle as CryptoPrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle as CryptoPublicKeyHandle

class TinkHybridCryptor : HybridCryptor {
  override fun encrypt(privateKey: CryptoPrivateKeyHandle, plaintext: ByteString): ByteString {
    require(privateKey is PrivateKeyHandle)
    // Cast privateKey to org.wfanet.measurement.common.crypto.tink.PrivateKeyHandle and use
    TODO("Not yet implemented")
  }

  override fun decrypt(publicKey: CryptoPublicKeyHandle, ciphertext: ByteString): ByteString {
    require(publicKey is PublicKeyHandle)
    // Cast publicKey to org.wfanet.measurement.common.crypto.tink.PublicKeyHandle and use
    TODO("Not yet implemented")
  }
}

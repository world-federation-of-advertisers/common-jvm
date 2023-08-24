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

package org.wfanet.measurement.common.crypto

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow

/** Handle to the public key of an encryption key pair. */
interface PublicKeyHandle {

  fun hybridEncrypt(plaintext: ByteString, contextInfo: ByteString? = null): ByteString

  suspend fun hybridEncrypt(plaintext: Flow<ByteString>, contextInfo: ByteString? = null): Flow<ByteString>

}

/** Handle to the private key of an encryption key pair. */
interface PrivateKeyHandle {
  val publicKey: PublicKeyHandle

  fun hybridDecrypt(ciphertext: ByteString, contextInfo: ByteString? = null): ByteString

  suspend fun hybridDecrypt(ciphertext: Flow<ByteString>, contextInfo: ByteString? = null): Flow<ByteString>

}

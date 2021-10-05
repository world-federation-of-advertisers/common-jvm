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

import com.google.crypto.tink.Aead as TinkAead
import com.google.protobuf.ByteString

/** Tink specific implementation of [Aead] */
class TinkAead(val aead: TinkAead) : Aead {
  /** @see encrypt */
  override fun encrypt(plaintext: ByteString): ByteString {
    return ByteString.copyFrom(encrypt(plaintext.toByteArray(), byteArrayOf()))
  }

  /**
   * Encrypts [plaintext].
   *
   * @param plaintext plaintext to be encrypted
   * @param associatedData associated data to be authenticated, but not encrypted. Associated data
   * is optional, so this parameter can be null. In this case the null value is equivalent to an
   * empty (zero-length) byte array. For successful decryption the same associatedData must be
   * provided along with the ciphertext
   * @return resulting ciphertext
   */
  override fun encrypt(plaintext: ByteArray?, associatedData: ByteArray?): ByteArray {
    return aead.encrypt(requireNotNull(plaintext), requireNotNull(associatedData))
  }

  /** @see decrypt */
  override fun decrypt(ciphertext: ByteString): ByteString {
    return ByteString.copyFrom(decrypt(ciphertext.toByteArray(), byteArrayOf()))
  }

  /**
   * Decrypts [ciphertext].
   *
   * @param ciphertext ciphertext to be decrypted
   * @param associatedData associated data to be authenticated. For successful decryption it must be
   * the same as associatedData used during encryption. Can be null, which is equivalent to an empty
   * (zero-length) byte array.
   * @return resulting plaintext
   */
  override fun decrypt(ciphertext: ByteArray?, associatedData: ByteArray?): ByteArray {
    return aead.decrypt(requireNotNull(ciphertext), requireNotNull(associatedData))
  }
}

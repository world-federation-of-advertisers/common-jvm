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

import com.google.crypto.tink.subtle.Hkdf
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.KeyMaterialGenerator

/**
 * Implementation of [KeyMaterialGenerator] that derives cryptographically secure key material using
 * the HKDF (HMAC-based Extract-and-Expand Key Derivation Function) with HMAC-SHA256.
 *
 * This class uses input keying material (IKM), an optional salt, and an info parameter to
 * deterministically generate key material of a specified size.
 *
 * The input keying material (ikm) is typically obtained from a secure source such as a Key
 * Management System (KMS) or a Message Authentication Code (MAC). The salt is optional but
 * recommended to add randomness. The info parameter provided to [generateKeyMaterial] is used as
 * context and should not be assumed secret.
 *
 * The derived key size defaults to 32 bytes, suitable for AES-256-GCM encryption keys.
 *
 * @property ikm The input keying material used as the initial secret for key derivation.
 * @property salt Optional salt value to add randomness to the key derivation process.
 * @property keySize The desired length of the derived key material in bytes. Default is 32 bytes.
 */
class KeyDerivation(
  private val ikm: ByteString, // input keying material (e.g. from KMS or MAC)
  private val salt: ByteString?,
  private val keySize: Int = 32, // 32 bytes for AES-256-GCM
) : KeyMaterialGenerator {

  /**
   * Generates deterministic, cryptographically secure key material using HKDF with HMAC-SHA256.
   *
   * @param info Context and application-specific information used in the key derivation. This input
   *   is NOT assumed to be secret.
   * @return A [ByteString] containing the derived key material of length [keySize].
   *
   * The security of the derived key does not depend on the secrecy of [info]. The HKDF construction
   * ensures the output key material is cryptographically strong and unique for different [info]
   * values.
   */
  override fun generateKeyMaterial(info: ByteString): ByteString {
    val hkdf =
      Hkdf.computeHkdf(
        "HmacSha256",
        ikm.toByteArray(),
        salt?.toByteArray(),
        info.toByteArray(),
        keySize,
      )
    return ByteString.copyFrom(hkdf)
  }
}

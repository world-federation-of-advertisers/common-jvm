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

package org.wfanet.measurement.common.crypto

import com.google.protobuf.ByteString

/**
 * Interface for generating cryptographically secure key material from a given input.
 *
 * Implementations of this interface must produce deterministic, cryptographically secure keys. The
 * input to [generateKeyMaterial] should NOT be assumed to be secret; it may be public or
 * predictable. Implementations should ensure that the generated key is secure even if the input is
 * known to an attacker.
 *
 * Different implementations may use different underlying algorithms or strategies to generate the
 * key material.
 */
interface KeyMaterialGenerator {
  /**
   * Generates deterministic, cryptographically secure key material from the provided input.
   *
   * @param input The input data used to derive the key material. This input is NOT assumed to be
   *   secret.
   * @return A ByteString containing the generated key material.
   *
   * Note: The security of the generated key must not depend on the secrecy of the input.
   * Implementations should use appropriate cryptographic primitives (e.g., HMAC, KDFs) to ensure
   * key security.
   */
  fun generateKeyMaterial(input: ByteString): ByteString
}

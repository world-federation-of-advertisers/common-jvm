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

/** Store of [PrivateKeyHandle]s. */
interface PrivateKeyStore<T : PrivateKeyHandle> {
  /**
   * Reads the [PrivateKeyHandle] with the specified [keyId] from storage.
   *
   * @return the [PrivateKeyHandle] or `null` if not found
   */
  suspend fun read(keyId: String): T?

  /**
   * Writes [privateKey] to storage.
   *
   * @return the ID (blob key) of the written [PrivateKeyHandle]
   */
  suspend fun write(privateKey: T): String
}

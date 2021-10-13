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

import com.google.crypto.tink.Aead
import org.wfanet.measurement.common.crypto.PrivateKeyStore as CryptoPrivateKeyStore
import org.wfanet.measurement.storage.Store

internal class PrivateKeyStore(private val store: Store<String>, private val aead: Aead) :
  CryptoPrivateKeyStore<TinkPrivateKeyHandle> {

  override suspend fun read(keyId: String): TinkPrivateKeyHandle? {
    TODO("Not yet implemented")
  }

  override suspend fun write(privateKey: TinkPrivateKeyHandle): String {
    TODO("Not yet implemented")
  }
}

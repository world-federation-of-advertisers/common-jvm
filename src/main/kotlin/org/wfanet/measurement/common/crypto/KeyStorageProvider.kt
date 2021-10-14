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

import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store

interface KeyStorageProvider<T : PrivateKeyHandle> {
  /**
   * Instantiates a [StorageClient] that wraps [storageClient] for storage of KMS-encrypted blobs.
   */
  fun makeKmsStorageClient(storageClient: StorageClient, keyUri: String): StorageClient

  /**
   * Instantiates a [PrivateKeyStore] for storage of implementation-specific KMS-encrypted private
   * key blobs.
   *
   * @param store [Store] where key blobs are stored
   * @param keyUri KMS master key URI for encrypting/decrypting key data
   */
  fun makeKmsPrivateKeyStore(store: Store<String>, keyUri: String): PrivateKeyStore<T>
}

package org.wfanet.measurement.common.crypto

import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store

interface KeyStorageProvider {
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
  fun makeKmsPrivateKeyStore(store: Store<String>, keyUri: String): PrivateKeyStore
}

package org.wfanet.panelmatch.client.storage

import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.crypto.KeyBlobStore
import org.wfanet.measurement.common.crypto.PrivateKeyStore.KeyId


interface HybridDecryptor {
  /**
   * Takes in an input and keyID and returns a decrypted Bytestring using HybridDecrypt.
   *
   * The keyId is used to retrieve the public key from a PrivateKeyStore. This is used to generate the KeysetHandle
   */
  suspend fun hybridDecrypt(
    input: ByteString,
    keyBlobStore: KeyBlobStore,
    keyId: KeyId,
  ): ByteString

  /**
   * Given a keyId, returns the appropriate KeysetHandle
   */
  suspend fun getPrivateKeysetHandle(
    keyBlobStore: KeyBlobStore,
    keyId: KeyId
  ): KeysetHandle
}

package org.wfanet.panelmatch.client.storage

import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.KeyBlobStore
import org.wfanet.measurement.common.crypto.PrivateKeyStore.KeyId


interface HybridEncryptor {
  /**
   * Takes in an input and keyID and returns an encrypted Bytestring using HybridEncrypt.
   *
   * The keyId is used to retrieve the public key from a PrivateKeyStore. This is used to generate the KeysetHandle
   */
  suspend fun hybridEncrypt(
    input: ByteString,
    keyBlobStore: KeyBlobStore,
    keyId: KeyId,
  ): ByteString

  /**
   * Given a keyId, returns the appropriate KeysetHandle
   */
  suspend fun getPublicKeysetHandle(
    keyBlobStore: KeyBlobStore,
    keyId: KeyId
  ): KeysetHandle
}

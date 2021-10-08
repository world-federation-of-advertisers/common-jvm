package org.wfanet.measurement.common.crypto

interface PrivateKeyStore {
  fun read(keyId: String): PrivateKeyHandle

  fun generate(): PrivateKeyHandle
}

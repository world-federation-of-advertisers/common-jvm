package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.Aead
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.storage.Store

private const val KEY_TEMPLATE = "ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM"

internal class EciesPrivateKeyStore(private val store: Store<String>, private val aead: Aead) :
  PrivateKeyStore {

  override fun read(keyId: String): PrivateKeyHandle {
    TODO("Not yet implemented")
  }

  override fun generate(): PrivateKeyHandle {
    TODO("Not yet implemented")
  }
}

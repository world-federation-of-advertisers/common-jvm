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

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.jwt.JwkSetConverter
import com.google.crypto.tink.jwt.JwtPublicKeySign
import com.google.crypto.tink.jwt.JwtPublicKeyVerify
import com.google.crypto.tink.jwt.JwtSignatureConfig
import com.google.crypto.tink.jwt.RawJwt
import com.google.crypto.tink.tinkkey.KeyAccess
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser

class PublicJwkHandle internal constructor(private val keysetHandle: KeysetHandle) {
  val verifier: JwtPublicKeyVerify = keysetHandle.getPrimitive(JwtPublicKeyVerify::class.java)

  fun getJwk(): JsonObject {
    val jwkSet =
      JsonParser.parseString(
          JwkSetConverter.fromKeysetHandle(keysetHandle, KeyAccess.publicAccess())
        )
        .asJsonObject
    return jwkSet.getAsJsonArray("keys").get(0).asJsonObject
  }

  companion object {
    fun fromJwk(jwk: JsonObject): PublicJwkHandle {
      val keyset = JsonObject()
      val keys = JsonArray()
      keys.add(jwk)
      keyset.add("keys", keys)

      return PublicJwkHandle(
        JwkSetConverter.toKeysetHandle(keyset.toString(), KeyAccess.publicAccess())
      )
    }
  }
}

class PrivateJwkHandle constructor(private val keysetHandle: KeysetHandle) {
  val publicKey = PublicJwkHandle(keysetHandle.publicKeysetHandle)

  fun sign(rawJwt: RawJwt): String {
    val signer = keysetHandle.getPrimitive(JwtPublicKeySign::class.java)
    return signer.signAndEncode(rawJwt)
  }

  companion object {
    init {
      JwtSignatureConfig.register()
    }

    private val RSA_KEY_TEMPLATE = KeyTemplates.get("JWT_RS256_2048_F4_RAW")

    /** Generates a new RSA key pair. */
    fun generateRsa(): PrivateJwkHandle {
      return PrivateJwkHandle(KeysetHandle.generateNew(RSA_KEY_TEMPLATE))
    }
  }
}

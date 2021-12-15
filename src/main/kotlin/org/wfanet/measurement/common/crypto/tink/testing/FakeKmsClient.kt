/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.crypto.tink.testing

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KmsClient
import java.security.GeneralSecurityException

class FakeKmsClient : KmsClient {
  private val keyAeads = mutableMapOf<String, Aead>()

  override fun doesSupport(keyUri: String?): Boolean {
    return keyUri?.startsWith(KEY_URI_PREFIX) ?: false
  }

  override fun withCredentials(credentialPath: String?): KmsClient {
    if (credentialPath == null) {
      return withDefaultCredentials()
    }
    throw UnsupportedOperationException("Not implemented")
  }

  override fun withDefaultCredentials(): KmsClient {
    return this
  }

  override fun getAead(keyUri: String?): Aead {
    if (!doesSupport(keyUri)) {
      throw GeneralSecurityException("URI not supported")
    }
    return keyAeads[keyUri] ?: throw GeneralSecurityException("Invalid key URI")
  }

  fun addAead(keyUri: String, aead: Aead) {
    require(doesSupport(keyUri)) { "URI not supported" }
    keyAeads[keyUri] = aead
  }

  companion object {
    const val KEY_URI_PREFIX = "fake-kms://"
  }
}

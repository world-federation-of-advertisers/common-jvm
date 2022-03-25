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

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.Store

/** Store of private signing keys. */
class SigningKeyStore(private val store: Store<Context>) {
  data class Context(val subjectKeyIdentifier: HexString) {
    constructor(subjectKeyIdentifier: ByteString) : this(HexString(subjectKeyIdentifier))

    val blobKey: String
      get() = subjectKeyIdentifier.value

    companion object {
      fun fromCertificate(certificate: X509Certificate): Context {
        return Context(requireNotNull(certificate.subjectKeyIdentifier))
      }
    }
  }

  internal suspend fun write(certificate: X509Certificate, privateKey: PrivateKey): String {
    val pemBytes =
      ByteString.newOutput().use { output ->
        val writer = PemWriter(output)
        withContext(Dispatchers.IO) {
          writer.write(certificate)
          writer.write(privateKey)
        }
        output.toByteString()
      }
    val blob = store.write(Context.fromCertificate(certificate), pemBytes)
    return blob.blobKey
  }

  suspend fun read(keyId: String): SigningKeyHandle? {
    val blob = store.get(keyId) ?: return null
    return PemReader(blob.read().flatten().newInput()).use { reader ->
      val certificate = reader.readCertificate()
      val privateKey = reader.readPrivateKeySpec().toPrivateKey(certificate.publicKey.algorithm)
      SigningKeyHandle(certificate, privateKey)
    }
  }
}

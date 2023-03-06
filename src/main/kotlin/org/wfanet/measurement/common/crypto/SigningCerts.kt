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
import java.io.File
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.toHexString

/** Certificates and associated private key for digital signatures. */
data class SigningCerts(
  val privateKeyHandle: SigningKeyHandle,
  /** [Map] of subject key identifier (SKID) to trusted [X509Certificate]. */
  val trustedCertificates: Map<ByteString, X509Certificate>
) {
  init {
    for ((skid, certificate) in trustedCertificates) {
      // If a cert lacks an AKID, it must be a root (self-signed) cert. We can default to the SKID.
      val akid = certificate.authorityKeyIdentifier ?: certificate.subjectKeyIdentifier!!
      require(skid == akid) {
        "Trusted certificate with subject key identifier (SKID) ${skid.toHexString()} is not a " +
          "root certificate authority (CA) certificate (AKID) ${akid.toHexString()}."
      }
    }
  }

  constructor(
    privateKeyHandle: SigningKeyHandle,
    trustedCertificates: Iterable<X509Certificate>
  ) : this(
    privateKeyHandle,
    trustedCertificates.associateBy {
      requireNotNull(it.subjectKeyIdentifier) {
        "Trusted certificate missing subject key identifier (SKID)"
      }
    }
  )

  companion object {
    fun fromPemFiles(
      certificateFile: File,
      privateKeyFile: File,
      trustedCertCollectionFile: File? = null
    ): SigningCerts {
      val certificate = readCertificate(certificateFile)
      val keyAlgorithm = certificate.publicKey.algorithm

      return SigningCerts(
        SigningKeyHandle(certificate, readPrivateKey(privateKeyFile, keyAlgorithm)),
        trustedCertCollectionFile?.let { readCertificateCollection(it) } ?: listOf()
      )
    }
  }
}

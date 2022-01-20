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

package org.wfanet.measurement.common.crypto.testing

import java.io.File
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.readPrivateKey

object SigningCertsTesting {
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

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
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString

/** Loads a signing private key from DER files. */
fun loadSigningKey(certificateDer: File, privateKeyDer: File): SigningKeyHandle {
  val certificate: X509Certificate =
    certificateDer.inputStream().use { input -> readCertificate(input) }
  return SigningKeyHandle(
    certificate,
    readPrivateKey(privateKeyDer.readByteString(), certificate.publicKey.algorithm)
  )
}

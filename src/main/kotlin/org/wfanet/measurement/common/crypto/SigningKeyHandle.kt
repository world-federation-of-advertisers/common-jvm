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
import java.security.Signature
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold

/** Handle to the private key of a signing key pair. */
data class SigningKeyHandle(
  override val keyId: String,
  val certificate: X509Certificate,
  private val privateKey: PrivateKey
) : KeyHandle {
  fun sign(data: ByteString): ByteString {
    val signature = newSigner().apply { update(data.asReadOnlyByteBuffer()) }.sign()
    return ByteString.copyFrom(signature)
  }

  fun newSigner(): Signature {
    val signer = Signature.getInstance(certificate.sigAlgName, jceProvider)
    signer.initSign(privateKey)
    return signer
  }
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and digitally
 * signs the accumulated values.
 *
 * @param keyHandle handle of signing private key
 * @return the digital signature of the accumulated values
 */
suspend inline fun Flow<ByteString>.collectAndSign(
  keyHandle: SigningKeyHandle,
  crossinline action: suspend (ByteString) -> Unit
): ByteString {
  val signer =
    fold(keyHandle.newSigner()) { signer, bytes ->
      action(bytes)
      signer.apply { update(bytes.asReadOnlyByteBuffer()) }
    }
  return ByteString.copyFrom(signer.sign())
}

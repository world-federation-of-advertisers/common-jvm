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
import com.google.protobuf.kotlin.toByteString
import java.security.PrivateKey
import java.security.Signature
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.storage.StorageClient

class SignedBlob(
  wrapped: StorageClient.Blob,
  val signature: ByteString,
  val algorithm: SignatureAlgorithm,
) : StorageClient.Blob by wrapped {

  /**
   * Reads the blob content as a flow, collecting it with [action] and verifying it against
   * [signature] using the specified [certificate].
   *
   * @return whether [signature] is valid for the blob content
   */
  suspend inline fun readAndVerify(
    certificate: X509Certificate,
    crossinline action: suspend (ByteString) -> Unit,
  ): Boolean {
    return read().collectAndVerify(certificate, algorithm, signature, action)
  }

  /**
   * Returns a [Flow] for the blob content which throws an [InvalidSignatureException] on collection
   * if [signature] is not valid for the blob content.
   */
  fun readVerifying(certificate: X509Certificate): Flow<ByteString> {
    return read().verifying(certificate, algorithm, signature)
  }
}

suspend fun StorageClient.createSignedBlob(
  blobKey: String,
  content: Flow<ByteString>,
  privateKey: PrivateKey,
  algorithm: SignatureAlgorithm,
): SignedBlob {
  val signer = privateKey.newSigner(algorithm)
  val outFlow = content.onEach(signer::update)
  val blob = writeBlob(blobKey, outFlow)
  val signature = signer.sign().toByteString()

  return SignedBlob(blob, signature, algorithm)
}

suspend fun StorageClient.createSignedBlob(
  blobKey: String,
  content: Flow<ByteString>,
  newSigner: () -> Signature,
): SignedBlob {
  val signer = newSigner()
  val outFlow = content.onEach(signer::update)
  val blob = writeBlob(blobKey, outFlow)
  val signature = signer.sign().toByteString()

  return SignedBlob(blob, signature, requireNotNull(signer.signatureAlgorithm))
}

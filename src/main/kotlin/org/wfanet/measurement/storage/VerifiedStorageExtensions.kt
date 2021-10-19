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

package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.crypto.SignedBlob
import org.wfanet.measurement.common.crypto.createSignedBlob
import org.wfanet.measurement.common.crypto.newSigner
import org.wfanet.measurement.storage.StorageClient.Blob

/**
 * Stub for verified read function. Intended to be used in combination with a the other party's
 * provided [X509Certificate], this validates that the data in the blob has been generated (or at
 * least signed as valid) by the other party.
 *
 * Note that the validation happens in a separate thread and is non-blocking, but will throw a
 * terminal error if it fails.
 */
@Deprecated("Use SignedBlob.readVerifying")
fun Blob.verifiedRead(
  cert: X509Certificate,
  signature: ByteString,
  bufferSize: Int
): Flow<ByteString> {
  return SignedBlob(this, signature).readVerifying(cert, bufferSize)
}

/**
 * Stub for verified write function. Intended to be used in combination with a provided [PrivateKey]
 * , this creates a signature in shared storage for the written blob that can be verified by the
 * other party using a pre-provided [X509Certificate].
 */
@Deprecated("Use org.wfanet.measurement.common.crypto.createSignedBlob")
suspend fun StorageClient.createSignedBlob(
  blobKey: String,
  content: Flow<ByteString>,
  privateKey: PrivateKey,
  cert: X509Certificate
): Pair<Blob, ByteString> {
  val signedBlob = createSignedBlob(blobKey, content) { privateKey.newSigner(cert) }
  return signedBlob to signedBlob.signature
}

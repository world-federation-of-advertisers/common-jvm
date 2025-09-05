// Copyright 2023 The Cross-Media Measurement Authors
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
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient

/** Store of blob and signature. */
class SignedStore(private val storageClient: StorageClient) {

  private fun blobKeyForSignature(blobKey: String): String {
    return "signature/$blobKey"
  }

  private fun blobKeyForContent(blobKey: String): String {
    return "content/$blobKey"
  }

  suspend fun write(
    blobKey: String,
    x509: X509Certificate,
    privateKey: PrivateKey,
    content: Flow<ByteString>
  ): String {
    val signature = privateKey.sign(x509, content.flatten())
    storageClient.writeBlob(blobKeyForContent(blobKey), content)
    storageClient.writeBlob(blobKeyForSignature(blobKey), signature)
    return blobKey
  }

  suspend fun read(blobKey: String, x509: X509Certificate): Flow<ByteString>? {
    val content = storageClient.getBlob(blobKeyForContent(blobKey)) ?: return null
    val signature = storageClient.getBlob(blobKeyForSignature(blobKey))
    return if (signature != null) {
      SignedBlob(content, signature.read().flatten()).readVerifying(x509)
    } else null
  }
}

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

package src.main.kotlin.org.wfanet.measurement.common.crypto

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle
import org.wfanet.measurement.common.crypto.SignedStore.BlobNotFoundException
import org.wfanet.measurement.common.crypto.sign
import org.wfanet.measurement.common.crypto.verifying
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient

private const val WRITE_BUFFER_SIZE = BYTES_PER_MIB * 5

class EncryptedSignedStore(private val storageClient: StorageClient) {

  private fun blobKeyForSignature(blobKey: String): String {
    return "signature/$blobKey"
  }

  private fun blobKeyForContent(blobKey: String): String {
    return "content/$blobKey"
  }
  //  PrivateKey and X509Certificate used to create the signature when writing the data
  //  PublicKey used to encrypt the content
  suspend fun write(
    blobKey: String,
    signingX509: X509Certificate,
    signingPrivateKey: PrivateKey,
    encryptingPublicKeyHandle: PublicKeyHandle,
    content: Flow<ByteString>
  ): String {
    val signature = signingPrivateKey.sign(signingX509, content.flatten()).asBufferedFlow(3)
//    val blob = storageClient.writeBlob(blobKey, content)
//    val combinedFlow = content.zip(signature) { a, b -> ByteString.EMPTY.concat(a).concat(ByteString.copyFromUtf8("_") ).concat(b) }
////    val signedBlob = SignedBlob(blob, signature)
//    println("joj signature: ${signature.flatten()}")
//    println("joj content: ${content.flatten()}")
//    val (a,b) = combinedFlow.
//    println("joj bytestring example: ${combinedFlow.flatten()}")
//    val encryptedData = encryptingPublicKeyHandle.hybridEncrypt(combinedFlow.flatten())
    val encryptedContent = encryptingPublicKeyHandle.hybridEncrypt(content)
//    keyHandle.getPrimitive(Aead::class.java)
//    val x = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
//    x.getPrimitive(Aead::class.java)
    val encryptedSignature = encryptingPublicKeyHandle.hybridEncrypt(signature.flatten())

    storageClient.writeBlob(blobKeyForSignature(blobKey), encryptedSignature)
    storageClient.writeBlob(blobKeyForContent(blobKey), encryptedContent)
    return blobKey
  }

  @Throws(BlobNotFoundException::class)
  suspend fun read(
    blobKey: String,
    signingX509: X509Certificate,
    decryptingPrivateKeyHandle: PrivateKeyHandle
  ): Flow<ByteString>? {
    val encryptedContent = storageClient.getBlob(blobKeyForContent(blobKey)) ?: return null
    val decryptedContent = decryptingPrivateKeyHandle
      .hybridDecrypt(encryptedContent.read())
    val encryptedSignature = storageClient.getBlob(blobKeyForSignature(blobKey)) ?: return null
    val decryptedSignature = decryptingPrivateKeyHandle
      .hybridDecrypt(encryptedSignature.read().flatten())
      .asBufferedFlow(WRITE_BUFFER_SIZE)
    return decryptedContent.verifying(signingX509, decryptedSignature.flatten())
  }
}

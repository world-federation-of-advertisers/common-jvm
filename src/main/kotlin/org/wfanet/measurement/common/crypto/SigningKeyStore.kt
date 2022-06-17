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
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toHexString
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store

/** Store of private signing keys. */
class SigningKeyStore(storageClient: StorageClient) {
  private val store: Store<Context> = BlobStore(storageClient)

  data class Context(val subjectKeyIdentifier: HexString) {
    val blobKey: String
      get() = subjectKeyIdentifier.value + ".pem"

    constructor(subjectKeyIdentifier: ByteString) : this(subjectKeyIdentifier.toHexString())

    companion object {
      fun fromCertificate(certificate: X509Certificate): Context {
        return Context(requireNotNull(certificate.subjectKeyIdentifier))
      }
    }
  }

  private class BlobStore(storageClient: StorageClient) : Store<Context>(storageClient) {
    override val blobKeyPrefix: String = "signing-keys"

    override fun deriveBlobKey(context: Context): String = context.blobKey
  }

  internal suspend fun write(certificate: X509Certificate, privateKey: PrivateKey): String {
    val pemBytes =
      ByteString.newOutput().use { output ->
        PemWriter(output).apply {
          @Suppress("BlockingMethodInNonBlockingContext") // Not blocking IO.
          write(certificate)
          @Suppress("BlockingMethodInNonBlockingContext") // Not blocking IO.
          write(privateKey)
        }
        output.toByteString()
      }
    val blob = store.write(Context.fromCertificate(certificate), pemBytes)
    return blob.blobKey
  }

  /**
   * Reads the [SigningKeyHandle] from [store] for the specified [blobContext].
   *
   * @return the [SigningKeyHandle], or `null` if not found
   */
  suspend fun read(blobContext: Context): SigningKeyHandle? {
    return store.get(blobContext)?.readSigningKeyHandle()
  }

  /**
   * Reads the [SigningKeyHandle] from [store] for the specified [blobKey].
   *
   * @return the [SigningKeyHandle], or `null` if not found
   */
  suspend fun read(blobKey: String): SigningKeyHandle? {
    return store.get(blobKey)?.readSigningKeyHandle()
  }

  companion object {
    private suspend fun Store.Blob.readSigningKeyHandle(): SigningKeyHandle {
      return PemReader(read().flatten().newInput()).use { reader ->
        val certificate = reader.readCertificate()
        val privateKey = reader.readPrivateKeySpec().toPrivateKey(certificate.publicKey.algorithm)
        SigningKeyHandle(certificate, privateKey)
      }
    }
  }
}

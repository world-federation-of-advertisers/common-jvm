package src.main.kotlin.org.wfanet.measurement.common.crypto

import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle
import org.wfanet.measurement.common.crypto.SignedStore
import org.wfanet.measurement.storage.StorageClient

class EncryptedSignedStore(private val storageClient: StorageClient) {

  private val signedStore: SignedStore = SignedStore(storageClient)

  //  PrivateKey and X509Certificate used to create the signature when writing the data
  //  PublicKey used to encrypt the content
  suspend fun write(
    blobKey: String,
    x509: X509Certificate,
    privateKey: PrivateKey,
    publicKeysetHandle: PublicKeyHandle,
    content: Flow<ByteString>
  ): String {
    val encryptedData = publicKeysetHandle.hybridEncrypt(content)
    return signedStore.write(blobKey, x509, privateKey, encryptedData)
  }

  suspend fun read(
    blobKey: String,
    x509: X509Certificate,
    privateKeysetHandle: PrivateKeyHandle
  ): Flow<ByteString>? {
    val encryptedContent = signedStore.read(blobKey, x509)
    return privateKeysetHandle.hybridDecrypt(encryptedContent)
  }
}

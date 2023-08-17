package src.main.kotlin.org.wfanet.measurement.common.crypto

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle
import org.wfanet.measurement.common.crypto.SignedStore
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient

private const val WRITE_BUFFER_SIZE = BYTES_PER_MIB * 5

class EncryptedSignedStore(private val storageClient: StorageClient) {

  private val signedStore: SignedStore = SignedStore(storageClient)

  //  PrivateKey and X509Certificate used to create the signature when writing the data
  //  PublicKey used to encrypt the content
  suspend fun write(
    blobKey: String,
    signingX509: X509Certificate,
    signingPrivateKey: PrivateKey,
    encryptingPublicKeyHandle: PublicKeyHandle,
    content: Flow<ByteString>
  ): String {
    val encryptedData = encryptingPublicKeyHandle.hybridEncrypt(content.flatten())
    return signedStore.write(
      blobKey,
      signingX509,
      signingPrivateKey,
      encryptedData.asBufferedFlow(WRITE_BUFFER_SIZE)
    )
  }

  suspend fun read(
    blobKey: String,
    signingX509: X509Certificate,
    decryptingPrivateKeyHandle: PrivateKeyHandle
  ): Flow<ByteString>? {
    val encryptedContent = signedStore.read(blobKey, signingX509)
    return decryptingPrivateKeyHandle
      .hybridDecrypt(encryptedContent.flatten())
      .asBufferedFlow(WRITE_BUFFER_SIZE)
  }
}

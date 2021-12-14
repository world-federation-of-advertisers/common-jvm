package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.testing.FIXED_ENCRYPTION_PRIVATE_KEYSET
import org.wfanet.measurement.common.crypto.testing.FIXED_ENCRYPTION_PUBLIC_KEYSET
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPublicKey

@RunWith(JUnit4::class)
class KeyHandleTest {
  @Test
  fun `generated private key can decrypt value`() {
    val privateKey = TinkPrivateKeyHandle.generateEcies()

    val cipherText = privateKey.publicKey.hybridEncrypt(PLAIN_TEXT_MESSAGE_BINARY)
    assertThat(privateKey.hybridDecrypt(cipherText)).isEqualTo(PLAIN_TEXT_MESSAGE_BINARY)
  }

  @Test
  fun `loaded private key can decrypt value`() {
    val privateKey = loadPrivateKey(FIXED_ENCRYPTION_PRIVATE_KEYSET)

    val cipherText = privateKey.publicKey.hybridEncrypt(PLAIN_TEXT_MESSAGE_BINARY)
    assertThat(privateKey.hybridDecrypt(cipherText)).isEqualTo(PLAIN_TEXT_MESSAGE_BINARY)
  }

  @Test
  fun `loaded private key can decrypt value encrypted by loaded public key`() {
    val privateKey = loadPrivateKey(FIXED_ENCRYPTION_PRIVATE_KEYSET)
    val publicKey = loadPublicKey(FIXED_ENCRYPTION_PUBLIC_KEYSET)

    val cipherText = publicKey.hybridEncrypt(PLAIN_TEXT_MESSAGE_BINARY)
    assertThat(privateKey.hybridDecrypt(cipherText)).isEqualTo(PLAIN_TEXT_MESSAGE_BINARY)
  }

  companion object {
    private const val PLAIN_TEXT_MESSAGE = "Lorem ipsum dolor sit amet"
    private val PLAIN_TEXT_MESSAGE_BINARY = PLAIN_TEXT_MESSAGE.toByteStringUtf8()
  }
}

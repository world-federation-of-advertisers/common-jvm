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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.hybrid.HybridConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.security.GeneralSecurityException
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle
import com.google.crypto.tink.StreamingAead
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlinx.coroutines.flow.forEach
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.flatten
import com.google.crypto.tink.streamingaead.StreamingAeadConfig;
import java.io.InputStreamReader
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.wfanet.measurement.common.asFlow
import kotlinx.coroutines.flow.flow


class TinkPublicKeyHandle internal constructor(internal val keysetHandle: KeysetHandle) :
  PublicKeyHandle {

  init {
    require(!keysetHandle.primaryKey().hasSecret())
  }

  constructor(serializedKeyset: ByteString) : this(parseKeyset(serializedKeyset))

  fun toByteString(): ByteString {
    return ByteString.newOutput().use {
      keysetHandle.writeNoSecret(BinaryKeysetWriter.withOutputStream(it))
      it.toByteString()
    }
  }

  override fun hybridEncrypt(plaintext: ByteString, contextInfo: ByteString?): ByteString {
    val hybridEncrypt: HybridEncrypt = keysetHandle.getPrimitive(HybridEncrypt::class.java)
    val ciphertext = hybridEncrypt.encrypt(plaintext.toByteArray(), contextInfo?.toByteArray())
    return ciphertext.toByteString()
  }

  override suspend fun hybridEncrypt(plaintext: Flow<ByteString>, contextInfo: ByteString?): Flow<ByteString> {
    val streamingAead = keysetHandle.getPrimitive(StreamingAead::class.java)
    return encryptFile(streamingAead, plaintext,contextInfo?.toByteArray())
  }

  private fun encryptFile(
    streamingAead: StreamingAead, input: Flow<ByteString>, associatedData: ByteArray?): Flow<ByteString> {
    val pipedOutputStream: PipedOutputStream = PipedOutputStream()
    val pipedInputStream: PipedInputStream =  PipedInputStream(pipedOutputStream)
    streamingAead.newEncryptingStream(pipedOutputStream, associatedData).use { ciphertextStream ->
      input.asBufferedFlow(1024).onEach { chunk ->
        ciphertextStream.write(chunk.toByteArray(), 0, chunk.size())
      }
    }

    return pipedInputStream.asFlow(1024)

//    streamingAead.newEncryptingStream(ByteArrayOutputStream(10), associatedData).use { ciphertextStream ->
//      ByteArrayInputStream(ba).use {plaintextStream ->
//        val chunk = ByteArray(1024)
//        var chunkLen = 0
//        while (plaintextStream.read(chunk).also { chunkLen = it } != -1) {
//          ciphertextStream.write(chunk, 0, chunkLen)
//        }
//      }
//    }
  }

  companion object {
    init {
      HybridConfig.register()
    }

    private fun parseKeyset(serialized: ByteString): KeysetHandle {
      return serialized.newInput().use {
        KeysetHandle.readNoSecret(BinaryKeysetReader.withInputStream(it))
      }
    }
  }
}

class TinkPrivateKeyHandle internal constructor(internal val keysetHandle: KeysetHandle) :
  PrivateKeyHandle {

  init {
    require(keysetHandle.primaryKey().hasSecret())
  }

  override val publicKey = TinkPublicKeyHandle(keysetHandle.publicKeysetHandle)

  override fun hybridDecrypt(ciphertext: ByteString, contextInfo: ByteString?): ByteString {
    val hybridDecrypt = keysetHandle.getPrimitive(HybridDecrypt::class.java)
    val plaintext = hybridDecrypt.decrypt(ciphertext.toByteArray(), contextInfo?.toByteArray())
    return plaintext.toByteString()
  }

  override suspend fun hybridDecrypt(ciphertext: Flow<ByteString>, contextInfo: ByteString?): Flow<ByteString> {
    val streamingAead = keysetHandle.getPrimitive(StreamingAead::class.java)
    return decryptFile(streamingAead, ciphertext, contextInfo?.toByteArray())
  }

  private suspend fun decryptFile(
    streamingAead: StreamingAead, input: Flow<ByteString>, associatedData: ByteArray?) = flow {
    val pipedOutputStream: PipedOutputStream = PipedOutputStream()
    val pipedInputStream: PipedInputStream =  PipedInputStream(pipedOutputStream)
    pipedOutputStream.use { ciphertextStream ->
      input.asBufferedFlow(1024).onEach { chunk ->
        ciphertextStream.write(chunk.toByteArray(), 0, chunk.size())
      }
    }
    val ciphertextStream: InputStream = streamingAead.newDecryptingStream(pipedInputStream, associatedData)
    val chunk = ByteArray(1024)
    var chunkLen = 0
    while (ciphertextStream.read(chunk).also { chunkLen = it } != -1) {
      emit(chunk.toByteString())
    }
    ciphertextStream.close()
  }

  companion object {
    init {
      HybridConfig.register()
      StreamingAeadConfig.register()
    }

    private val ECIES_KEY_TEMPLATE = KeyTemplates.get("ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM")
    private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM_HKDF_1MB")


    /** Generates a new ECIES key pair. */
    fun generateEcies(streaming:Boolean = false): TinkPrivateKeyHandle {
      return if(streaming)
        TinkPrivateKeyHandle(KeysetHandle.generateNew(AEAD_KEY_TEMPLATE))
      else
        TinkPrivateKeyHandle(KeysetHandle.generateNew(ECIES_KEY_TEMPLATE))
    }
  }
}

/** Loads a private key from a cleartext binary Tink Keyset. */
fun loadPrivateKey(binaryKeyset: File): TinkPrivateKeyHandle {
  return TinkPrivateKeyHandle(CleartextKeysetHandle.read(BinaryKeysetReader.withFile(binaryKeyset)))
}

/** Loads a public key from a cleartext binary Tink Keyset. */
fun loadPublicKey(binaryKeyset: File): TinkPublicKeyHandle {
  return TinkPublicKeyHandle(KeysetHandle.readNoSecret(BinaryKeysetReader.withFile(binaryKeyset)))
}

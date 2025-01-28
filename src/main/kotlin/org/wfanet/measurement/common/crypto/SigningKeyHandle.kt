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
import io.netty.handler.ssl.SslContextBuilder
import java.security.PrivateKey
import java.security.Signature
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient

/**
 * Handle to the private key of a signing key pair.
 *
 * @throws IllegalArgumentException if there is no [SignatureAlgorithm] for [privateKey] that is
 *   compatible with [DEFAULT_HASH_ALGORITHM]
 */
data class SigningKeyHandle(val certificate: X509Certificate, private val privateKey: PrivateKey) {
  /** Default [SignatureAlgorithm] for [privateKey] using [DEFAULT_HASH_ALGORITHM]. */
  val defaultAlgorithm =
    requireNotNull(SignatureAlgorithm.fromKeyAndHashAlgorithm(privateKey, DEFAULT_HASH_ALGORITHM)) {
      "Cannot determine signature algorithm for (${privateKey.algorithm}, $DEFAULT_HASH_ALGORITHM)"
    }

  /**
   * Signs [data] using the specified [algorithm].
   *
   * @return the signature
   */
  fun sign(algorithm: SignatureAlgorithm, data: ByteString) = privateKey.sign(algorithm, data)

  /**
   * Signs [data] using the [SignatureAlgorithm] specified in [certificate].
   *
   * Note that this may result in an error as the [SignatureAlgorithm] may not actually be
   * compatible with the [certificate] key.
   */
  @Deprecated("Specify algorithm explicitly")
  @Suppress("DEPRECATION")
  fun sign(data: ByteString): ByteString = privateKey.sign(certificate, data)

  /** Returns a new [Signature] initialized for signing with [algorithm]. */
  fun newSigner(algorithm: SignatureAlgorithm) = privateKey.newSigner(algorithm)

  /**
   * Returns a new [Signature] initialized for signing using the [SignatureAlgorithm] specified in
   * [certificate].
   *
   * Note that this may result in an error as the [SignatureAlgorithm] may not actually be
   * compatible with the [certificate] key.
   */
  @Deprecated("Specify algorithm explicitly")
  @Suppress("DEPRECATION")
  fun newSigner(): Signature = privateKey.newSigner(certificate)

  /**
   * Writes this [SigningKeyHandle] to [keyStore].
   *
   * @return the blob key
   */
  suspend fun write(keyStore: SigningKeyStore): String {
    return keyStore.write(certificate, privateKey)
  }

  /** Returns a new server [SslContextBuilder] using this key handle. */
  fun newServerSslContextBuilder(): SslContextBuilder =
    SslContextBuilder.forServer(privateKey, certificate)

  companion object {
    val DEFAULT_HASH_ALGORITHM = HashAlgorithm.SHA256

    /** @see SslContextBuilder.keyManager */
    fun SslContextBuilder.keyManager(keyHandle: SigningKeyHandle): SslContextBuilder =
      keyManager(keyHandle.privateKey, keyHandle.certificate)
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
  algorithm: SignatureAlgorithm,
  crossinline action: suspend (ByteString) -> Unit,
): ByteString = collectAndSign({ keyHandle.newSigner(algorithm) }, action)

/**
 * Terminal flow operator that collects the given flow with the provided [action] and digitally
 * signs the accumulated values.
 *
 * @param keyHandle handle of signing private key
 * @return the digital signature of the accumulated values
 */
@Deprecated("Specify algorithm explicitly")
@Suppress("DEPRECATION")
suspend inline fun Flow<ByteString>.collectAndSign(
  keyHandle: SigningKeyHandle,
  crossinline action: suspend (ByteString) -> Unit,
): ByteString = collectAndSign(keyHandle::newSigner, action)

suspend fun StorageClient.createSignedBlob(
  blobKey: String,
  content: Flow<ByteString>,
  signingKey: SigningKeyHandle,
  algorithm: SignatureAlgorithm,
): SignedBlob = createSignedBlob(blobKey, content) { signingKey.newSigner(algorithm) }

@Deprecated("Specify algorithm explicitly")
@Suppress("DEPRECATION")
suspend fun StorageClient.createSignedBlob(
  blobKey: String,
  content: Flow<ByteString>,
  signingKey: SigningKeyHandle,
): SignedBlob = createSignedBlob(blobKey, content, signingKey::newSigner)

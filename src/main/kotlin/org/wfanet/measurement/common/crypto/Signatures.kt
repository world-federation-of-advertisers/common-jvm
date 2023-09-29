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
import java.security.Key
import java.security.PrivateKey
import java.security.Signature
import java.security.cert.CertPathValidator
import java.security.cert.CertPathValidatorException
import java.security.cert.PKIXParameters
import java.security.cert.TrustAnchor
import java.security.cert.X509Certificate
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach

private const val CERT_PATH_VALIDATOR_ALGORITHM = "PKIX"

enum class HashAlgorithm(
  /** OID in dot notation */
  val oid: String
) {
  // From RFC 4055
  SHA256("2.16.840.1.101.3.4.2.1"),
  SHA384("2.16.840.1.101.3.4.2.2"),
  SHA512("2.16.840.1.101.3.4.2.3")
}

enum class SignatureAlgorithm(
  /** OID in dot notation */
  val oid: String,
  /** Java Security standard algorithm name */
  val javaName: String
) {
  // From RFC 5758
  ECDSA_WITH_SHA256("1.2.840.10045.4.3.2", "SHA256withECDSA"),
  ECDSA_WITH_SHA384("1.2.840.10045.4.3.3", "SHA384withECDSA"),
  ECDSA_WITH_SHA512("1.2.840.10045.4.3.4", "SHA512withECDSA"),

  // From RFC 4055,
  SHA_256_WITH_RSA_ENCRYPTION("1.2.840.113549.1.1.11", "SHA256withRSA"),
  SHA_384_WITH_RSA_ENCRYPTION("1.2.840.113549.1.1.12", "SHA384withRSA"),
  SHA_512_WITH_RSA_ENCRYPTION("1.2.840.113549.1.1.13", "SHA512withRSA");

  companion object {
    fun fromOid(oid: String): SignatureAlgorithm? {
      return SignatureAlgorithm.values().find { it.oid == oid }
    }

    fun fromJavaName(javaName: String): SignatureAlgorithm? {
      return SignatureAlgorithm.values().find { it.javaName == javaName }
    }

    /**
     * Returns a [SignatureAlgorithm] that is compatible with the specified [key] and
     * [hashAlgorithm], or `null` if not found.
     *
     * Note that there may be multiple compatible [SignatureAlgorithm] values. This just returns one
     * such value.
     */
    fun fromKeyAndHashAlgorithm(key: Key, hashAlgorithm: HashAlgorithm): SignatureAlgorithm? {
      return when (key.algorithm) {
        "EC" ->
          when (hashAlgorithm) {
            HashAlgorithm.SHA256 -> ECDSA_WITH_SHA256
            HashAlgorithm.SHA384 -> ECDSA_WITH_SHA384
            HashAlgorithm.SHA512 -> ECDSA_WITH_SHA512
          }
        "RSA" ->
          when (hashAlgorithm) {
            HashAlgorithm.SHA256 -> SHA_256_WITH_RSA_ENCRYPTION
            HashAlgorithm.SHA384 -> SHA_384_WITH_RSA_ENCRYPTION
            HashAlgorithm.SHA512 -> SHA_512_WITH_RSA_ENCRYPTION
          }
        else -> null
      }
    }
  }
}

object Signatures {
  inline fun sign(data: ByteString, newSigner: () -> Signature): ByteString {
    return newSigner().apply { update(data) }.sign().toByteString()
  }

  inline fun verify(
    data: ByteString,
    signature: ByteString,
    newVerifier: () -> Signature
  ): Boolean {
    return newVerifier().apply { update(data) }.verify(signature.toByteArray())
  }
}

/** @see [Signature.update] */
fun Signature.update(bytes: ByteString) {
  for (buffer in bytes.asReadOnlyByteBufferList()) {
    update(buffer)
  }
}

val X509Certificate.signatureAlgorithm: SignatureAlgorithm?
  get() = SignatureAlgorithm.fromOid(sigAlgOID)

val Signature.signatureAlgorithm: SignatureAlgorithm?
  get() = SignatureAlgorithm.fromJavaName(algorithm)

/**
 * Returns a new [Signature] instance that has been initialized for signing.
 *
 * @param algorithm Java security signature algorithm name
 * @throws java.security.NoSuchAlgorithmException if no implementation is found for the specified
 *   algorithm in [jceProvider]
 * @throws java.security.InvalidKeyException if the [PrivateKey] is invalid, including if it is not
 *   compatible with the algorithm
 */
fun PrivateKey.newSigner(algorithm: SignatureAlgorithm): Signature {
  return Signature.getInstance(algorithm.javaName, jceProvider).also { it.initSign(this) }
}

/**
 * Returns a new [Signature] instance that has been initialized for signing using the signature
 * algorithm specified in [certificate].
 *
 * @throws java.security.NoSuchAlgorithmException if no implementation is found for the specified
 *   algorithm in [jceProvider]
 * @throws java.security.InvalidKeyException if the [PrivateKey] is invalid, including if it is not
 *   compatible with the algorithm
 */
@Deprecated("Specify algorithm explicitly")
fun PrivateKey.newSigner(certificate: X509Certificate): Signature {
  val algorithm = requireNotNull(certificate.signatureAlgorithm)
  return newSigner(algorithm)
}

/** Signs [data] using this [PrivateKey]. */
fun PrivateKey.sign(algorithm: SignatureAlgorithm, data: ByteString): ByteString {
  return Signatures.sign(data) { newSigner(algorithm) }
}

/** Signs [data] using this [PrivateKey] and the signature algorithm specified in [certificate]. */
@Deprecated("Specify algorithm explicitly")
fun PrivateKey.sign(certificate: X509Certificate, data: ByteString): ByteString {
  val algorithm = requireNotNull(certificate.signatureAlgorithm)
  return sign(algorithm, data)
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and digitally
 * signs the accumulated values.
 *
 * @return the digital signature of the accumulated values
 */
suspend inline fun Flow<ByteString>.collectAndSign(
  newSigner: () -> Signature,
  crossinline action: suspend (ByteString) -> Unit
): ByteString {
  val signer = newSigner()
  collect { bytes ->
    action(bytes)
    signer.update(bytes)
  }
  return signer.sign().toByteString()
}

/**
 * Signs [data] using this [PrivateKey]. Takes in a [Flow<ByteString>] for signing streaming data.
 *
 * Note that the deferred output (the signature) will only be ready when the Flow has been fully
 * collected.
 *
 * @param certificate the [X509Certificate] that can be used to verify the signature
 */
@Deprecated("Use Flow<ByteString>.collectAndSign or StorageClient.createSignedBlob")
fun PrivateKey.signFlow(
  certificate: X509Certificate,
  data: Flow<ByteString>,
  algorithm: SignatureAlgorithm = requireNotNull(certificate.signatureAlgorithm),
): Pair<Flow<ByteString>, Deferred<ByteString>> {
  val deferredSig = CompletableDeferred<ByteString>()
  val outFlow = flow {
    val signature = data.collectAndSign({ newSigner(algorithm) }) { emit(it) }
    deferredSig.complete(signature)
  }

  return outFlow to deferredSig
}

/** Returns a new [Signature] instance that has been initialized for verification. */
fun X509Certificate.newVerifier(algorithm: SignatureAlgorithm): Signature {
  return Signature.getInstance(algorithm.javaName, jceProvider).also { it.initVerify(this) }
}

/** Returns a new [Signature] instance that has been initialized for verification. */
@Deprecated("Specify algorithm explicitly")
fun X509Certificate.newVerifier(): Signature {
  val algorithm = checkNotNull(signatureAlgorithm)
  return newVerifier(algorithm)
}

/**
 * Verifies that the [signature] for [data] was signed by the entity represented by this
 * [X509Certificate].
 */
fun X509Certificate.verifySignature(
  algorithm: SignatureAlgorithm,
  data: ByteString,
  signature: ByteString
): Boolean {
  return Signatures.verify(data, signature) { newVerifier(algorithm) }
}

/**
 * Verifies that the [signature] for [data] was signed by the entity represented by this
 * [X509Certificate].
 */
@Deprecated("Specify algorithm explicitly")
fun X509Certificate.verifySignature(data: ByteString, signature: ByteString): Boolean {
  val algorithm = checkNotNull(signatureAlgorithm)
  return verifySignature(algorithm, data, signature)
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and verifies the
 * digital [signature] of the accumulated values.
 *
 * @return whether the signature was verified
 */
suspend inline fun Flow<ByteString>.collectAndVerify(
  signature: ByteString,
  newVerifier: () -> Signature,
  crossinline action: suspend (ByteString) -> Unit
): Boolean {
  val verifier = newVerifier()
  collect { bytes ->
    action(bytes)
    verifier.update(bytes)
  }
  return verifier.verify(signature.toByteArray())
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and verifies the
 * digital [signature] of the accumulated values.
 *
 * @return whether the signature was verified
 */
suspend inline fun Flow<ByteString>.collectAndVerify(
  certificate: X509Certificate,
  algorithm: SignatureAlgorithm,
  signature: ByteString,
  crossinline action: suspend (ByteString) -> Unit
): Boolean {
  return collectAndVerify(signature, { certificate.newVerifier(algorithm) }, action)
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and verifies the
 * digital [signature] of the accumulated values.
 *
 * @return whether the signature was verified
 */
@Deprecated("Specify algorithm explicitly")
suspend inline fun Flow<ByteString>.collectAndVerify(
  certificate: X509Certificate,
  signature: ByteString,
  crossinline action: suspend (ByteString) -> Unit
): Boolean {
  val algorithm = requireNotNull(certificate.signatureAlgorithm)
  return collectAndVerify(certificate, algorithm, signature, action)
}

inline fun Flow<ByteString>.verifying(
  signature: ByteString,
  newVerifier: () -> Signature,
): Flow<ByteString> {
  val verifier = newVerifier()
  return onEach { bytes -> verifier.update(bytes) }
    .onCompletion { e ->
      if (e == null && !verifier.verify(signature.toByteArray())) {
        throw InvalidSignatureException("Signature is invalid")
      }
    }
}

/**
 * Intermediate [ByteString] flow operator which applies digital signature verification.
 *
 * Upon collection of the returned flow, it will throw an [InvalidSignatureException] if the
 * [signature] was not created from the accumulated flow values by the entity represented by
 * [certificate].
 */
fun Flow<ByteString>.verifying(
  certificate: X509Certificate,
  algorithm: SignatureAlgorithm,
  signature: ByteString,
): Flow<ByteString> {
  return verifying(signature) { certificate.newVerifier(algorithm) }
}

/**
 * Intermediate [ByteString] flow operator which applies digital signature verification.
 *
 * Upon collection of the returned flow, it will throw an [InvalidSignatureException] if the
 * [signature] was not created from the accumulated flow values by the entity represented by
 * [certificate].
 */
@Deprecated("Specify algorithm explicitly")
fun Flow<ByteString>.verifying(
  certificate: X509Certificate,
  signature: ByteString,
): Flow<ByteString> {
  val algorithm = requireNotNull(certificate.signatureAlgorithm)
  return verifying(certificate, algorithm, signature)
}

/**
 * Returns a flow containing the original values of Flow [data] and verifies that the [signature]
 * for [data] was signed by the entity represented by this [X509Certificate].
 *
 * The output is the downstream Flow of [data]. If [data] is found to not match [signature] upon
 * collecting the flow, the flow will throw an [InvalidSignatureException].
 */
@Deprecated(
  "Use Flow<ByteString>.verifying",
  ReplaceWith("data.verifying(this, algorithm, signature)")
)
fun X509Certificate.verifySignedFlow(
  data: Flow<ByteString>,
  signature: ByteString,
  algorithm: SignatureAlgorithm = checkNotNull(signatureAlgorithm),
): Flow<ByteString> = data.verifying(this, algorithm, signature)

/**
 * Validates this [X509Certificate], ensuring that it was issued by [trustedIssuer].
 *
 * Note that this does not check for certificate revocation.
 *
 * @throws CertPathValidatorException if the certificate does not validate
 */
fun X509Certificate.validate(trustedIssuer: X509Certificate) {
  val trustAnchor = TrustAnchor(trustedIssuer, null)
  val validator = CertPathValidator.getInstance(CERT_PATH_VALIDATOR_ALGORITHM)
  validator.validate(
    generateCertPath(listOf(this)),
    PKIXParameters(setOf(trustAnchor)).also { it.isRevocationEnabled = false }
  )
}

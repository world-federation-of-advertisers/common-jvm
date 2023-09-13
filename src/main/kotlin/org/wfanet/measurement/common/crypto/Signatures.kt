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

/** @see [Signature.update] */
fun Signature.update(bytes: ByteString) {
  for (buffer in bytes.asReadOnlyByteBufferList()) {
    update(buffer)
  }
}

/** Returns a new [Signature] instance that has been initialized for signing. */
fun PrivateKey.newSigner(certificate: X509Certificate): Signature {
  val signer = Signature.getInstance(certificate.sigAlgName, jceProvider)
  signer.initSign(this)
  return signer
}

/**
 * Signs [data] using this [PrivateKey].
 *
 * @param certificate the [X509Certificate] that can be used to verify the signature
 */
fun PrivateKey.sign(certificate: X509Certificate, data: ByteString): ByteString {
  return newSigner(certificate).apply { update(data) }.sign().toByteString()
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
  data: Flow<ByteString>
): Pair<Flow<ByteString>, Deferred<ByteString>> {
  val deferredSig = CompletableDeferred<ByteString>()
  val outFlow = flow {
    val signature = data.collectAndSign({ newSigner(certificate) }) { emit(it) }
    deferredSig.complete(signature)
  }

  return outFlow to deferredSig
}

/** Returns a new [Signature] instance that has been initialized for verification. */
fun X509Certificate.newVerifier(): Signature {
  return Signature.getInstance(sigAlgName, jceProvider).apply { initVerify(this@newVerifier) }
}

/**
 * Verifies that the [signature] for [data] was signed by the entity represented by this
 * [X509Certificate].
 */
fun X509Certificate.verifySignature(data: ByteString, signature: ByteString): Boolean {
  return newVerifier().apply { update(data) }.verify(signature.toByteArray())
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and verifies the
 * digital [signature] of the accumulated values.
 *
 * @return whether the signature was verified
 */
suspend inline fun Flow<ByteString>.collectAndVerify(
  certificate: X509Certificate,
  signature: ByteString,
  crossinline action: suspend (ByteString) -> Unit
): Boolean {
  val verifier = certificate.newVerifier()
  collect { bytes ->
    action(bytes)
    verifier.update(bytes)
  }
  return verifier.verify(signature.toByteArray())
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
  signature: ByteString
): Flow<ByteString> {
  val verifier = certificate.newVerifier()
  return onEach { bytes -> verifier.update(bytes) }
    .onCompletion { e ->
      if (e == null && !verifier.verify(signature.toByteArray())) {
        throw InvalidSignatureException("Signature is invalid")
      }
    }
}

/**
 * Returns a flow containing the original values of Flow [data] and verifies that the [signature]
 * for [data] was signed by the entity represented by this [X509Certificate].
 *
 * The output is the downstream Flow of [data]. If [data] is found to not match [signature] upon
 * collecting the flow, the flow will throw an [InvalidSignatureException].
 */
@Deprecated("Use Flow<ByteString>.verifying")
fun X509Certificate.verifySignedFlow(
  data: Flow<ByteString>,
  signature: ByteString,
): Flow<ByteString> = data.verifying(this, signature)

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

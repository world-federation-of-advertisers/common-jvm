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
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.Provider
import java.security.Security
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.KeySpec
import java.security.spec.PKCS8EncodedKeySpec
import kotlin.jvm.Throws
import org.conscrypt.Conscrypt

private const val CERTIFICATE_TYPE = "X.509"
private const val SUBJECT_KEY_IDENTIFIER_OID = "2.5.29.14"
private const val AUTHORITY_KEY_IDENTIFIER_OID = "2.5.29.35"

/**
 * Known ASN.1 DER tags.
 *
 * See
 * [A Layman's Guide to a Subset of ASN.1, BER, and DER](https://luca.ntop.org/Teaching/Appunti/asn1.html)
 */
private enum class Asn1Tag(val byte: Byte) {
  /** Universal tag for OCTET_STRING type. */
  OCTET_STRING(0x04.toByte()),
  /** Constructed tag for SEQUENCE type. */
  SEQUENCE(0x30.toByte()),
  /** Context-specific tag for keyIdentifier within AuthorityKeyIdentifier extension. */
  KEY_IDENTIFIER(0x80.toByte())
}

private val conscryptProvider: Provider =
  Conscrypt.newProvider().also { Security.insertProviderAt(it, 1) }

/** Primary JCE [Provider] for crypto operations. */
val jceProvider: Provider = conscryptProvider

private val certFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE, jceProvider)

/**
 * Reads an X.509 certificate from a PEM file.
 *
 * @throws java.security.cert.CertificateException on parsing errors
 */
fun readCertificate(pemFile: File): X509Certificate = readCertificate(pemFile::inputStream)

/**
 * Reads an X.509 certificate from a DER-encoded [ByteString].
 *
 * @throws java.security.cert.CertificateException on parsing errors
 */
fun readCertificate(der: ByteString): X509Certificate = readCertificate(der::newInput)

fun readCertificate(input: InputStream): X509Certificate {
  return certFactory.generateCertificate(input) as X509Certificate
}

private inline fun readCertificate(newInputStream: () -> InputStream): X509Certificate {
  return newInputStream().use { readCertificate(it) }
}

/**
 * Reads an X.509 certificate collection from a PEM file.
 *
 * @throws java.security.cert.CertificateException on parsing errors
 */
@Throws(IOException::class)
fun readCertificateCollection(pemFile: File): Collection<X509Certificate> {
  @Suppress("UNCHECKED_CAST") // Underlying mutable collection never exposed.
  return pemFile.inputStream().use { fileInputStream ->
    certFactory.generateCertificates(fileInputStream)
  } as
    Collection<X509Certificate>
}

/**
 * Reads a private key from DER-encoded PKCS#8 [ByteString]
 *
 * @throws java.security.spec.InvalidKeySpecException on parsing errors
 */
fun readPrivateKey(data: ByteString, algorithm: String): PrivateKey {
  return PKCS8EncodedKeySpec(data.toByteArray()).toPrivateKey(algorithm)
}

/** Creates a [PrivateKey] from this [KeySpec]. */
fun KeySpec.toPrivateKey(algorithm: String): PrivateKey {
  return KeyFactory.getInstance(algorithm, jceProvider).generatePrivate(this)
}

/**
 * The keyIdentifier from the SubjectKeyIdentifier (AKI) X.509 extension, or `null` if it cannot be
 * found.
 */
val X509Certificate.subjectKeyIdentifier: ByteString?
  get() {
    val extension: ByteArray = getExtensionValue(SUBJECT_KEY_IDENTIFIER_OID) ?: return null
    if (extension.size < 4 || extension[2] != Asn1Tag.OCTET_STRING.byte) {
      return null
    }

    val length = extension[3].toInt() // Assuming short form, where length <= 127 bytes.
    return ByteString.copyFrom(extension, 4, length)
  }

/**
 * The keyIdentifier from the AuthorityKeyIdentifier (AKI) X.509 extension, or `null` if it cannot
 * be found.
 */
val X509Certificate.authorityKeyIdentifier: ByteString?
  get() {
    val extension: ByteArray = getExtensionValue(AUTHORITY_KEY_IDENTIFIER_OID) ?: return null
    if (extension.size < 6 ||
        extension[2] != Asn1Tag.SEQUENCE.byte ||
        extension[4] != Asn1Tag.KEY_IDENTIFIER.byte
    ) {
      return null
    }

    val length = extension[5].toInt() // Assuming short form, where length <= 127 bytes.
    return ByteString.copyFrom(extension, 6, length)
  }

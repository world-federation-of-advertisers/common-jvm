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

import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.security.PrivateKey
import java.security.PublicKey
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import kotlin.jvm.Throws

private const val LINE_FEED: Byte = 0x0A
private const val LINE_FEED_INT: Int = LINE_FEED.toInt()
private const val BEGIN_PREFIX = "-----BEGIN "
private const val END_PREFIX = "-----END "
private const val SUFFIX = "-----"
private const val LINE_LENGTH = 64
private val CHARSET = StandardCharsets.US_ASCII
private val base64Encoder = Base64.getMimeEncoder(LINE_LENGTH, byteArrayOf(LINE_FEED))

private enum class PemType(private val header: String) {
  CERTIFICATE("CERTIFICATE"),
  PRIVATE_KEY("PRIVATE KEY"),
  PUBLIC_KEY("PUBLIC KEY");

  val begin = BEGIN_PREFIX + header + SUFFIX
  val end = END_PREFIX + header + SUFFIX
}

/**
 * Writer for outputting objects in PEM format.
 *
 * See [RFC 7468](https://datatracker.ietf.org/doc/html/rfc7468)
 */
class PemWriter(private val output: OutputStream) : AutoCloseable {
  @Throws(IOException::class)
  private fun write(type: PemType, der: ByteArray) {
    with(output) {
      write(type.begin.toByteArray(CHARSET))
      write(LINE_FEED_INT)
      write(base64Encoder.encode(der))
      write(LINE_FEED_INT)
      write(type.end.toByteArray(CHARSET))
      write(LINE_FEED_INT)
    }
  }

  @Throws(IOException::class)
  fun write(certificate: X509Certificate) {
    write(PemType.CERTIFICATE, certificate.encoded)
  }

  @Throws(IOException::class)
  fun write(privateKey: PrivateKey) {
    write(PemType.PRIVATE_KEY, privateKey.encoded)
  }

  @Throws(IOException::class)
  fun write(publicKey: PublicKey) {
    write(PemType.PUBLIC_KEY, publicKey.encoded)
  }

  @Throws(IOException::class)
  override fun close() {
    output.close()
  }
}

/**
 * Reader for inputting objects in PEM format.
 *
 * See [RFC 7468](https://datatracker.ietf.org/doc/html/rfc7468)
 */
class PemReader private constructor(private val reader: BufferedReader) : AutoCloseable {
  constructor(input: InputStream) : this(input.bufferedReader(CHARSET))

  constructor(inputReader: InputStreamReader) : this(inputReader.buffered()) {
    require(inputReader.encoding == CHARSET.name())
  }

  fun readCertificate(): X509Certificate {
    val der = checkNotNull(read(PemType.CERTIFICATE))
    return ByteArrayInputStream(der).use { readCertificate(it) }
  }

  fun readPrivateKeySpec(): PKCS8EncodedKeySpec {
    val der = checkNotNull(read(PemType.PRIVATE_KEY))
    return PKCS8EncodedKeySpec(der)
  }

  private fun read(type: PemType): ByteArray? {
    val header = readHeader() ?: return null
    check(header == type.begin) { "Expected \"${type.begin}\", got \"$header\"" }

    val buffer = StringBuffer()
    while (true) {
      val line = reader.readLine() ?: error("Unexpected end of file")
      if (line.startsWith(END_PREFIX)) {
        return buffer.toString().base64Decode()
      }
      buffer.append(line)
    }
  }

  private fun readHeader(): String? {
    var line: String
    do {
      line = reader.readLine() ?: return null
    } while (line.isBlank())
    return line
  }

  override fun close() {
    reader.close()
  }
}

/** Reads a private key from a PKCS#8-encoded PEM file. */
@Throws(IOException::class)
fun readPrivateKey(pemFile: File, algorithm: String): PrivateKey {
  return PemReader(pemFile.inputStream()).use { reader ->
    reader.readPrivateKeySpec().toPrivateKey(algorithm)
  }
}

/** Decodes this base64-encoded [String] to a [ByteArray]. */
private fun String.base64Decode(): ByteArray = Base64.getDecoder().decode(this)

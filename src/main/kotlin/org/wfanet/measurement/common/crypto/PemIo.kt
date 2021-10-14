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

import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import kotlin.jvm.Throws
import org.wfanet.measurement.common.base64Decode

private const val LINE_FEED: Byte = 0x0A
private const val BEGIN_PREFIX = "-----BEGIN "
private const val END_PREFIX = "-----END "
private const val SUFFIX = "-----"
private const val LINE_LENGTH = 64
private val CHARSET = StandardCharsets.US_ASCII
private val base64Encoder = Base64.getMimeEncoder(LINE_LENGTH, byteArrayOf(LINE_FEED))

internal enum class PemType(private val header: String) {
  CERTIFICATE("CERTIFICATE"),
  PRIVATE_KEY("PRIVATE KEY");

  val begin = BEGIN_PREFIX + header + SUFFIX
  val end = END_PREFIX + header + SUFFIX
}

class PemWriter(private val output: OutputStream) : AutoCloseable {
  @Throws(IOException::class)
  private fun write(type: PemType, der: ByteArray) {
    with(output) {
      write(type.begin.toByteArray(CHARSET))
      write(LINE_FEED.toInt())
      write(base64Encoder.encode(der))
      write(LINE_FEED.toInt())
      write(type.end.toByteArray(CHARSET))
      write(LINE_FEED.toInt())
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
  override fun close() {
    output.close()
  }
}

class PemReader(lines: Sequence<String>) {
  private val lineIterator = lines.iterator()

  fun readCertificate(): X509Certificate {
    val der = checkNotNull(read(PemType.CERTIFICATE))
    return ByteArrayInputStream(der).use { readCertificate(it) }
  }

  fun readPrivateKeySpec(): PKCS8EncodedKeySpec {
    val der = checkNotNull(read(PemType.PRIVATE_KEY))
    return PKCS8EncodedKeySpec(der)
  }

  private fun read(type: PemType): ByteArray? {
    val firstLine = readLine() ?: return null
    check(firstLine == type.begin) { "Expected \"${type.begin}\", got \"$firstLine\"" }

    val buffer = StringBuffer()
    while (true) {
      val line = readLine() ?: error("Unexpected end of file")
      if (line.startsWith(END_PREFIX)) {
        return buffer.toString().base64Decode()
      }
      buffer.append(line)
    }
  }

  /**
   * Reads the next non-blank line.
   *
   * @return the line, or `null` if no more lines
   */
  private fun readLine(): String? {
    if (!lineIterator.hasNext()) {
      return null
    }
    val next = lineIterator.next()
    return next.ifBlank { readLine() }
  }
}

/** Reads a private key from a PKCS#8-encoded PEM file. */
@Throws(IOException::class)
fun readPrivateKey(pemFile: File, algorithm: String): PrivateKey {
  return pemFile.reader().useLines { lines ->
    PemReader(lines).readPrivateKeySpec().toPrivateKey(algorithm)
  }
}

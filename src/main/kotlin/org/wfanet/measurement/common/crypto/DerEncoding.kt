/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.crypto

import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.cert.X509Certificate
import kotlin.experimental.and
import org.wfanet.measurement.common.toStringHex

private const val SUBJECT_KEY_IDENTIFIER_OID = "2.5.29.14"
private const val AUTHORITY_KEY_IDENTIFIER_OID = "2.5.29.35"

private enum class Asn1TagClass {
  UNIVERSAL,
  APPLICATION,
  CONTEXT_SPECIFIC,
  PRIVATE;

  companion object {
    @OptIn(ExperimentalUnsignedTypes::class)
    fun fromIdentifier(octet: Byte): Asn1TagClass =
      when (val tagClass = octet.toUByte().toInt() ushr 6) {
        0 -> UNIVERSAL
        1 -> APPLICATION
        2 -> CONTEXT_SPECIFIC
        3 -> PRIVATE
        else -> error("Failed to parse ASN.1 tag class from identifier octet $octet: $tagClass")
      }
  }
}

/** ASN.1 universal tags. */
private enum class Asn1Tag(val byte: Byte) {
  OCTET_STRING(0x04.toByte()),
  SEQUENCE(0x10.toByte()),
}

/** ASN.1 context-specific tags for the X.509 v3 AuthorityKeyIdentifier extension. */
private enum class AuthorityKeyIdentifierTag(val byte: Byte) {
  KEY_IDENTIFIER(0x00.toByte()),
}

/**
 * Descriptor for a definite-length DER-encoded value.
 *
 * See
 * [A Layman's Guide to a Subset of ASN.1, BER, and DER](https://luca.ntop.org/Teaching/Appunti/asn1.html)
 */
private data class DefiniteLengthDescriptor(
  val tagClass: Asn1TagClass,
  /**
   * ASN.1 tag number.
   *
   * Note that tag numbers above 30 (0x1E) are not supported.
   */
  val tagNumber: Byte,
  val contentLength: Int,
  val contentOffset: Int,
) {
  fun checkTag(universalTag: Asn1Tag) {
    checkTag(Asn1TagClass.UNIVERSAL, universalTag.byte)
  }

  fun checkTag(tagClass: Asn1TagClass, tagNumber: Byte) {
    check(this.tagClass == tagClass && this.tagNumber == tagNumber) {
      val expectedTagNumber = "0x${tagNumber.toStringHex()}"
      val actualTagNumber = "0x${this.tagNumber.toStringHex()}"
      "Expected tag $expectedTagNumber with class $tagClass, " +
        "actual tag $actualTagNumber with class ${this.tagClass}"
    }
  }

  companion object {
    private const val TAG_NUMBER_MASK = 0b0001_1111.toByte()
    private const val LENGTH_OCTETS_MASK = 0b0111_1111.toByte()

    fun parse(octets: ByteArray, offset: Int = 0): DefiniteLengthDescriptor {
      val tagNumber: Byte = octets[offset] and TAG_NUMBER_MASK
      check(tagNumber != TAG_NUMBER_MASK) { "High tag numbers not supported" }
      val tagClass = Asn1TagClass.fromIdentifier(octets[offset])
      val lengthIndex: Int = offset + 1

      if (octets[lengthIndex].takeHighestOneBit() != 0b1000_0000.toByte()) {
        // Short length form.
        return DefiniteLengthDescriptor(
          tagClass,
          tagNumber,
          octets[lengthIndex].toInt(),
          lengthIndex + 1
        )
      }

      val numAdditionalLengthOctets: Int = (octets[lengthIndex] and LENGTH_OCTETS_MASK).toInt()
      check(numAdditionalLengthOctets <= 4) { "Lengths over 32 bits not supported" }
      val buffer =
        if (numAdditionalLengthOctets == 4) {
          ByteBuffer.wrap(octets, lengthIndex + 1, 4)
        } else {
          // Pad buffer to 4 bytes.
          ByteBuffer.allocate(4).apply {
            repeat(4 - numAdditionalLengthOctets) { put(0x00) }
            put(octets, lengthIndex + 1, numAdditionalLengthOctets)
            flip()
          }
        }
      return DefiniteLengthDescriptor(
        tagClass,
        tagNumber,
        buffer.order(ByteOrder.BIG_ENDIAN).int,
        lengthIndex + numAdditionalLengthOctets + 1
      )
    }
  }
}

/**
 * The keyIdentifier from the SubjectKeyIdentifier (SKI) X.509 extension, or `null` if it cannot be
 * found.
 */
val X509Certificate.subjectKeyIdentifier: ByteString?
  get() {
    val octets: ByteArray = getExtensionValue(SUBJECT_KEY_IDENTIFIER_OID) ?: return null
    val extensionInfo =
      DefiniteLengthDescriptor.parse(octets).apply { checkTag(Asn1Tag.OCTET_STRING) }
    val keyIdentifierInfo =
      DefiniteLengthDescriptor.parse(octets, extensionInfo.contentOffset).apply {
        checkTag(Asn1Tag.OCTET_STRING)
      }
    return ByteString.copyFrom(
      octets,
      keyIdentifierInfo.contentOffset,
      keyIdentifierInfo.contentLength
    )
  }

/**
 * The keyIdentifier from the AuthorityKeyIdentifier (AKI) X.509 extension, or `null` if it cannot
 * be found.
 */
val X509Certificate.authorityKeyIdentifier: ByteString?
  get() {
    val octets: ByteArray = getExtensionValue(AUTHORITY_KEY_IDENTIFIER_OID) ?: return null
    val extensionInfo =
      DefiniteLengthDescriptor.parse(octets).apply { checkTag(Asn1Tag.OCTET_STRING) }
    val akidInfo =
      DefiniteLengthDescriptor.parse(octets, extensionInfo.contentOffset).apply {
        checkTag(Asn1Tag.SEQUENCE)
      }
    val sequenceElementInfo =
      DefiniteLengthDescriptor.parse(octets, akidInfo.contentOffset).apply {
        check(tagClass == Asn1TagClass.CONTEXT_SPECIFIC)
      }

    if (sequenceElementInfo.tagNumber != AuthorityKeyIdentifierTag.KEY_IDENTIFIER.byte) {
      // keyIdentifier is an optional field in the AuthorityKeyIdentifier sequence.
      return null
    }
    return ByteString.copyFrom(
      octets,
      sequenceElementInfo.contentOffset,
      sequenceElementInfo.contentLength
    )
  }

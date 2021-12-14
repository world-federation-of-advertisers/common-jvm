// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.util.Base64

private val urlEncoder = Base64.getUrlEncoder().withoutPadding()
private val urlDecoder = Base64.getUrlDecoder()

/** Encodes this [ByteString] into an RFC 7515 base64url-encoded [String]. */
fun ByteString.base64UrlEncode(): String = encode(urlEncoder)

/** Decodes this RFC 7515 base64url-encoded [String] into a [ByteString]. */
fun String.base64UrlDecode(): ByteString = decode(urlDecoder)

/** Encodes [ByteArray] with RFC 7515's Base64url encoding into a base-64 string. */
fun ByteArray.base64UrlEncode(): String = urlEncoder.encodeToString(this)

fun Long.base64UrlEncode(): String {
  return toReadOnlyByteBuffer().encode(urlEncoder)
}

private fun ByteString.encode(encoder: Base64.Encoder): String {
  return asReadOnlyByteBuffer().encode(encoder)
}

private fun ByteBuffer.encode(encoder: Base64.Encoder): String {
  return String(encoder.encode(this).array(), Charsets.ISO_8859_1)
}

private fun String.decode(decoder: Base64.Decoder): ByteString {
  return ByteString.readFrom(decoder.wrap(byteInputStream(Charsets.ISO_8859_1)))
}

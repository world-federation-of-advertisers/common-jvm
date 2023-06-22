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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import kotlinx.coroutines.flow.Flow

private const val SHA_256 = "SHA-256"

object Hashing {
  /** Computes the SHA-256 hash of [data]. */
  fun hashSha256(data: ByteString): ByteString {
    return newSha256Hasher().digest(data)
  }

  /**
   * Computes the SHA-256 hash of [data].
   *
   * @param byteOrder the byte order to use when converting [data] to bytes
   */
  fun hashSha256(data: Long, byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): ByteString {
    val buffer = ByteBuffer.allocate(8).order(byteOrder).putLong(data).asReadOnlyBuffer()
    buffer.flip()
    return newSha256Hasher().apply { update(buffer) }.digest().toByteString()
  }

  @PublishedApi
  internal fun newSha256Hasher(): MessageDigest {
    return MessageDigest.getInstance(SHA_256)
  }
}

@Deprecated("Use Hashing.hashSha256", ReplaceWith("Hashing.hashSha256(data)"))
fun hashSha256(data: ByteString): ByteString = Hashing.hashSha256(data)

@Deprecated("Use Hashing.hashSha256", ReplaceWith("Hashing.hashSha256(data, byteOrder)"))
fun hashSha256(data: Long, byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): ByteString =
  Hashing.hashSha256(data, byteOrder)

/**
 * Terminal flow operator that collects the given flow with the provided [action] and computes the
 * SHA-256 hash of the accumulated values.
 *
 * @return the SHA-256 hash of the accumulated values
 */
suspend inline fun Flow<ByteString>.collectAndHashSha256(
  crossinline action: suspend (ByteString) -> Unit
): ByteString {
  val hasher = Hashing.newSha256Hasher()
  collect { bytes ->
    action(bytes)
    hasher.update(bytes)
  }
  return hasher.digest().toByteString()
}

@PublishedApi
internal fun MessageDigest.digest(data: ByteString): ByteString {
  update(data)
  return digest().toByteString()
}

fun MessageDigest.update(data: ByteString) {
  for (buffer in data.asReadOnlyByteBufferList()) {
    update(buffer)
  }
}

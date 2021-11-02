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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.toByteString

private const val SHA_256 = "SHA-256"

/** Computes the SHA-256 hash of [data]. */
fun hashSha256(data: ByteString): ByteString {
  return newSha256Hasher().digest(data)
}

/** Computes the SHA-256 hash of [data]. */
fun hashSha256(data: Long): ByteString {
  val buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(data).asReadOnlyBuffer()
  buffer.flip()
  return newSha256Hasher().apply { update(buffer) }.digest().toByteString()
}

/**
 * Terminal flow operator that collects the given flow with the provided [action] and computes the
 * SHA-256 hash of the accumulated values.
 *
 * @return the SHA-256 hash of the accumulated values
 */
suspend inline fun Flow<ByteString>.collectAndHashSha256(
  crossinline action: suspend (ByteString) -> Unit
): ByteString {
  val hasher = newSha256Hasher()
  collect { bytes ->
    action(bytes)
    hasher.update(bytes)
  }
  return hasher.digest().toByteString()
}

@PublishedApi
internal fun newSha256Hasher(): MessageDigest {
  return MessageDigest.getInstance(SHA_256)
}

@PublishedApi
internal fun MessageDigest.digest(data: ByteString): ByteString {
  update(data)
  return digest().toByteString()
}

@PublishedApi
internal fun MessageDigest.update(data: ByteString) {
  for (buffer in data.asReadOnlyByteBufferList()) {
    update(buffer)
  }
}

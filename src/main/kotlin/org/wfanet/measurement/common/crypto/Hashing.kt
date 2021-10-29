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
import java.security.MessageDigest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.toByteString

@PublishedApi internal const val SHA_256 = "SHA-256"

/** Generates a SHA-256 of [data]. */
fun hashSha256(data: ByteString): ByteString {
  return MessageDigest.getInstance(SHA_256).digest(data)
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
  val hasher = MessageDigest.getInstance(SHA_256)
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

@PublishedApi
internal fun MessageDigest.update(data: ByteString) {
  for (buffer in data.asReadOnlyByteBufferList()) {
    update(buffer)
  }
}

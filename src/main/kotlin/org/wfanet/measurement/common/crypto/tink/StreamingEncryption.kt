/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.Key
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.streamingaead.AesCtrHmacStreamingKey
import com.google.crypto.tink.streamingaead.AesGcmHkdfStreamingKey
import com.google.crypto.tink.streamingaead.StreamingAeadKey
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.CoroutineReadableByteChannel
import org.wfanet.measurement.common.CoroutineWritableByteChannel

object StreamingEncryption {
  /**
   * Encrypts [plaintext] using [encryptionKey] into a sequence of chunks of size [chunkSizeBytes].
   *
   * @param encryptionKey key to encrypt [plaintext] with. This must be compatible with the
   *   [StreamingAead] primitive.
   * @param plaintext data to encrypt
   * @param chunkSizeBytes size of output chunks in bytes
   * @param associatedData optional associated data for AEAD
   * @return a [Sequence] of ciphertext chunks, where all but the last element will have
   *   [size][ByteString.size] == [chunkSizeBytes] and the last element will have
   *   [size][ByteString.size] <= [chunkSizeBytes].
   */
  fun encryptChunked(
    encryptionKey: KeysetHandle,
    plaintext: ByteString,
    associatedData: ByteString?,
    chunkSizeBytes: Int,
  ): Sequence<ByteString> = sequence {
    val primaryKey: Key = encryptionKey.primary.key
    require(primaryKey is StreamingAeadKey) { "Unsupported key type" }
    // Ensure a large enough buffer to avoid blocking.
    val pipeBufferSize: Int =
      chunkSizeBytes.coerceAtLeast(getCiphertextSegmentSizeBytes(primaryKey))

    val plaintextSource: ByteBuffer = plaintext.asReadOnlyByteBuffer()
    val streamingAead = encryptionKey.getPrimitive(StreamingAead::class.java)
    val outputBuffer = ByteBuffer.allocate(chunkSizeBytes)

    BufferedPipe(pipeBufferSize).use { pipe ->
      streamingAead.newEncryptingChannel(pipe.sink, associatedData?.toByteArray()).use {
        encryptingChannel ->
        while (plaintextSource.hasRemaining()) {
          // Write in chunks to avoid blocking behavior due to filling pipe buffer.
          val sourceSlice =
            plaintextSource.slice().limit(chunkSizeBytes.coerceAtMost(plaintextSource.remaining()))
          val written = encryptingChannel.write(sourceSlice)
          plaintextSource.position(plaintextSource.position() + written)

          // Read.
          yieldChunked(pipe.source, outputBuffer)
        }
      }

      // The encrypting channel might do some final writes to the sink on close, so we need to read
      // here too.
      yieldChunked(pipe.source, outputBuffer)

      // Output final chunk if there's more remaining.
      outputBuffer.flip()
      if (outputBuffer.hasRemaining()) {
        yield(outputBuffer.toByteString())
        outputBuffer.clear()
      }
    }
  }

  private fun getCiphertextSegmentSizeBytes(key: StreamingAeadKey): Int {
    return when (key) {
      is AesGcmHkdfStreamingKey -> key.parameters.ciphertextSegmentSizeBytes
      is AesCtrHmacStreamingKey -> key.parameters.ciphertextSegmentSizeBytes
      else -> throw IllegalArgumentException("Unsupported key type")
    }
  }
}

/** Encrypts [plaintext] to a [Flow] of ciphertext chunks. */
fun StreamingAead.encrypt(
  plaintext: Flow<ByteString>,
  associatedData: ByteString?,
  coroutineContext: @BlockingExecutor CoroutineContext,
): Flow<ByteString> {
  return channelFlow {
      val ciphertextDestination = CoroutineWritableByteChannel.createBlocking(channel)
      newEncryptingChannel(ciphertextDestination, associatedData?.toByteArray()).use {
        encryptingChannel: WritableByteChannel ->
        plaintext.collect { plaintextChunk: ByteString ->
          for (sourceBuffer: ByteBuffer in plaintextChunk.asReadOnlyByteBufferList()) {
            encryptingChannel.write(sourceBuffer)
          }
        }
      }
    }
    .buffer(Channel.RENDEZVOUS)
    .flowOn(coroutineContext)
}

/** Decrypts [ciphertext] to a [Flow] of plaintext chunks. */
fun StreamingAead.decrypt(
  ciphertext: Flow<ByteString>,
  associatedData: ByteString?,
  chunkSizeBytes: Int,
): Flow<ByteString> {
  return flow {
    coroutineScope {
      val ciphertextSourceChannel: ReceiveChannel<ByteString> =
        ciphertext.buffer(Channel.RENDEZVOUS).produceIn(this)
      val ciphertextSource = CoroutineReadableByteChannel(ciphertextSourceChannel)
      val outputBuffer = ByteBuffer.allocate(chunkSizeBytes)
      newDecryptingChannel(ciphertextSource, associatedData?.toByteArray()).use {
        decryptingChannel: ReadableByteChannel ->
        do {
          yield() // Let channel producer run.
          val readCount: Int = decryptingChannel.read(outputBuffer)
          if (!outputBuffer.hasRemaining() || readCount == -1) {
            emit(outputBuffer.flip().toByteString())
            outputBuffer.clear()
          }
        } while (readCount != -1)
      }
    }
  }
}

private suspend fun SequenceScope<ByteString>.yieldChunked(
  channel: ReadableByteChannel,
  outputBuffer: ByteBuffer,
) {
  while (channel.read(outputBuffer) > 0) {
    if (!outputBuffer.hasRemaining()) {
      yield(outputBuffer.flip().toByteString())
      outputBuffer.clear()
    }
  }
}

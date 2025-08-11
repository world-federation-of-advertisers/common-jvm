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
import com.google.crypto.tink.streamingaead.AesGcmHkdfStreamingKey
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

object StreamingEncryption {
  /**
   * Encrypts [plaintext] using [encryptionKey] into a sequence of chunks of size [chunkSizeBytes].
   */
  fun encryptChunked(
    encryptionKey: KeysetHandle,
    plaintext: ByteString,
    chunkSizeBytes: Int,
    associatedData: ByteString? = null,
  ): Sequence<ByteString> = sequence {
    val primaryKey: Key = encryptionKey.primary.key
    require(primaryKey is AesGcmHkdfStreamingKey) { "Unsupported key type" }
    val pipeBufferSize =
      chunkSizeBytes.coerceAtLeast(primaryKey.parameters.ciphertextSegmentSizeBytes)

    val plaintextSource = plaintext.asReadOnlyByteBuffer()
    val streamingAead = encryptionKey.getPrimitive(StreamingAead::class.java)
    val outputBuffer = ByteBuffer.allocate(chunkSizeBytes)

    BufferedPipe(pipeBufferSize).use { pipe ->
      streamingAead.newEncryptingChannel(pipe.sink, associatedData?.toByteArray()).use {
        encryptingChannel ->
        while (plaintextSource.hasRemaining()) {
          // Write.
          val sourceSlice =
            plaintextSource.slice().limit(chunkSizeBytes.coerceAtMost(plaintextSource.remaining()))
          val written = encryptingChannel.write(sourceSlice)
          plaintextSource.position(plaintextSource.position() + written)

          // Read.
          yieldChunked(pipe.source, outputBuffer)
        }
      }

      // The encrypting channel might do some final writes to destination after it's closed, so we
      // need to read out from destination after.
      yieldChunked(pipe.source, outputBuffer)

      // Output final chunk if there's more remaining.
      outputBuffer.flip()
      if (outputBuffer.hasRemaining()) {
        yield(outputBuffer.toByteString())
        outputBuffer.clear()
      }
    }
  }
}

/** Similar to [java.nio.channels.Pipe], but with a buffer of known [capacity]. */
private class BufferedPipe(capacity: Int) : AutoCloseable {
  private val buffer = ByteBuffer.allocate(capacity)
  private val lock = ReentrantLock()
  private val bufferHasRemaining = lock.newCondition()

  /** A channel representing the readable end of a pipe. */
  val source =
    object : ReadableByteChannel {
      private var closed = false

      override fun read(dst: ByteBuffer): Int {
        if (!isOpen) {
          throw ClosedChannelException()
        }

        lock.withLock {
          buffer.flip()
          return dst.tryPut(buffer).also {
            buffer.compact()
            if (buffer.hasRemaining()) {
              bufferHasRemaining.signal()
            }
          }
        }
      }

      override fun isOpen(): Boolean = !closed

      override fun close() {
        closed = true
      }
    }

  /** A channel representing the writable end of a pipe. */
  val sink =
    object : WritableByteChannel {
      private var closed = false

      override fun write(src: ByteBuffer): Int {
        if (!isOpen) {
          throw ClosedChannelException()
        }

        var written = 0
        lock.withLock {
          while (src.hasRemaining()) {
            while (buffer.remaining() < src.remaining()) {
              bufferHasRemaining.await()
            }
            written += buffer.tryPut(src)
          }
        }

        return written
      }

      override fun isOpen(): Boolean = !closed

      override fun close() {
        closed = true
      }
    }

  override fun close() {
    source.close()
    sink.close()
    buffer.clear()
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

/**
 * Copies as many bytes as will fit from [src] to this buffer.
 *
 * @return the number of bytes copied
 */
private fun ByteBuffer.tryPut(src: ByteBuffer): Int {
  if (!hasRemaining() || !src.hasRemaining()) {
    return 0
  }

  val remainingCapacity: Int = remaining()
  val srcRemaining: Int = src.remaining()
  return if (srcRemaining > remainingCapacity) {
    val slice = src.slice().limit(remainingCapacity)
    put(slice)
    src.position(src.position() + remainingCapacity)
    remainingCapacity
  } else {
    put(src)
    srcRemaining
  }
}

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

import com.google.crypto.tink.StreamingAead
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.ReadableByteChannel

private const val MIN_AEAD_BUFFER_SIZE = 1 * 1024 * 1024

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

/** Encrypts [plaintext] in output chunks of size [chunkSizeBytes]. */
fun StreamingAead.encryptChunked(
  plaintext: ByteString,
  chunkSizeBytes: Int,
  associatedData: ByteString? = null,
): Sequence<ByteString> = sequence {
  val source = plaintext.asReadOnlyByteBuffer()

  ByteBufferChannel(chunkSizeBytes.coerceAtLeast(MIN_AEAD_BUFFER_SIZE)).use { destination ->
    val outputBuffer = ByteBuffer.allocate(chunkSizeBytes)

    newEncryptingChannel(destination, associatedData?.toByteArray()).use { encryptingChannel ->
      while (source.hasRemaining()) {
        // Note that the encrypting channel has some unexpected behavior where its write method does
        // not attempt to write as much as it can to the destination. It will instead internally
        // buffer and finish flushing to destination when closed.
        encryptingChannel.write(source)
        yieldChunked(destination, outputBuffer)
      }
    }

    // The encrypting channel might do some final writes to destination after it's closed, so we
    // need to read out from destination after.
    yieldChunked(destination, outputBuffer)

    outputBuffer.flip()
    if (outputBuffer.hasRemaining()) {
      yield(outputBuffer.toByteString())
      outputBuffer.clear()
    }
    // The encrypting channel will close the destination when it is closed, so we can't clear the
    // destination on close. We instead have to have a separate clear.
    destination.clear()
  }
}

/**
 * Non-blocking [ByteChannel] that wraps a [ByteBuffer] with [capacity].
 *
 * This channel can be read after it has been closed.
 */
private class ByteBufferChannel(capacity: Int) : ByteChannel {
  private val buffer = ByteBuffer.allocate(capacity)
  private var open = true

  override fun write(src: ByteBuffer): Int {
    if (!open) {
      throw ClosedChannelException()
    }

    return buffer.tryPut(src)
  }

  override fun read(dst: ByteBuffer): Int {
    if (!dst.hasRemaining()) {
      return 0
    }

    buffer.flip()
    return dst.tryPut(buffer).also { buffer.compact() }
  }

  override fun isOpen(): Boolean = open

  override fun close() {
    open = false
  }

  fun clear() {
    buffer.clear()
    close()
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

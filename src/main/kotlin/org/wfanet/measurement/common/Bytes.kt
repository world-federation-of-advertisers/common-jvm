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
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.time.Instant
import java.util.Stack
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.isActive
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor

const val BYTES_PER_MIB = 1024 * 1024

@Deprecated(
  "Use com.google.protobuf.kotlin.toByteString",
  ReplaceWith("toByteString()", "com.google.protobuf.kotlin.toByteString"),
)
fun ByteArray.toByteString(): ByteString = toByteString()

fun Iterable<ByteArray>.toByteString(): ByteString {
  val totalSize = sumOf { it.size }

  return ByteString.newOutput(totalSize).use { output ->
    forEach { output.write(it) }
    output.toByteString()
  }
}

/** Returns a [ByteString] which is the concatenation of all elements. */
fun Iterable<ByteString>.flatten(): ByteString {
  return fold(ByteString.EMPTY) { acc, value -> acc.concat(value) }
}

/** Copies all bytes in a list of [ByteString]s into a [ByteArray]. */
fun Iterable<ByteString>.toByteArray(): ByteArray {
  // Allocate a ByteBuffer large enough for all the bytes in all the byte strings.
  val buffer = ByteBuffer.allocate(sumOf { it.size })
  forEach { byteString -> byteString.copyTo(buffer) }
  return buffer.array()
}

/** @see ByteString.size(). */
val ByteString.size: Int
  get() = size()

fun ByteString.toLong(byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): Long {
  require(size == 8) { "Expected 8 bytes, got $size" }
  return asReadOnlyByteBuffer().order(byteOrder).long
}

/**
 * Converts this [Long] to a [ByteString].
 *
 * @param byteOrder the byte order to use when converting the [Long] to bytes
 */
fun Long.toByteString(byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): ByteString {
  return ByteString.copyFrom(toReadOnlyByteBuffer(byteOrder))
}

/**
 * Converts this [Long] to a [ByteBuffer].
 *
 * @param byteOrder the byte order to use when converting the [Long] to bytes. Note that this does
 *   not affect the [order][ByteBuffer.order] of the returned [ByteBuffer], which is always
 *   [ByteOrder.BIG_ENDIAN].
 */
fun Long.toReadOnlyByteBuffer(byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): ByteBuffer {
  val buffer = ByteBuffer.allocate(8).order(byteOrder).putLong(this)
  buffer.flip()
  return buffer.asReadOnlyBuffer()
}

/**
 * Converts this [Instant] to a [ByteBuffer] containing the concatenation of
 * [Instant.getEpochSecond] and [Instant.getNano].
 *
 * @param byteOrder the byte order to use when converting the [Instant] to bytes. Note that this
 *   does not affect the [order][ByteBuffer.order] of the returned [ByteBuffer], which is always
 *   [ByteOrder.BIG_ENDIAN].
 */
fun Instant.toReadOnlyByteBuffer(byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN): ByteBuffer {
  return ByteBuffer.allocate(8 + 4)
    .apply {
      order(byteOrder)
      putLong(epochSecond)
      putInt(nano)
      flip()
    }
    .asReadOnlyBuffer()
}

/**
 * Returns a [ByteString] with the same contents, padded with zeros in its most-significant bits
 * until it reaches the specified size.
 *
 * If this [ByteString]'s size is already at least the specified size, it will be returned instead
 * of a new one.
 *
 * @param paddedSize the size of the padded [ByteString]
 */
fun ByteString.withPadding(paddedSize: Int): ByteString {
  if (size >= paddedSize) {
    return this
  }

  return ByteString.newOutput(paddedSize)
    .use { output ->
      repeat(paddedSize - size) { output.write(0x00) }
      output.toByteString()
    }
    .concat(this)
}

fun ByteString.withTrailingPadding(paddedSize: Int): ByteString {
  if (size >= paddedSize) {
    return this
  }

  val padding = ByteArray(paddedSize - size)
  return concat(ByteString.copyFrom(padding))
}

/** Returns a [ByteString] containing the specified elements. */
fun byteStringOf(vararg bytesAsInts: Int): ByteString {
  return ByteString.newOutput(bytesAsInts.size).use {
    for (byteAsInt in bytesAsInts) {
      it.write(byteAsInt)
    }
    it.toByteString()
  }
}

/**
 * Returns a [ByteString] which is the concatenation of the elements.
 *
 * This is a terminal [Flow] operation.
 */
suspend fun Flow<ByteString>.flatten(): ByteString {
  return fold(ByteString.EMPTY) { acc, value -> acc.concat(value) }
}

suspend fun Flow<ByteString>.toByteArray(): ByteArray = flatten().toByteArray()

/**
 * Creates a flow that produces [ByteString] values with the specified [size][bufferSize] from this
 * [ByteArray].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 */
fun ByteArray.asBufferedFlow(bufferSize: Int): Flow<ByteString> =
  ByteBuffer.wrap(this).asBufferedFlow(bufferSize)

/**
 * Creates a flow that produces [ByteString] values with the specified [size][bufferSize] from this
 * [ByteBuffer].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 */
fun ByteBuffer.asBufferedFlow(bufferSize: Int): Flow<ByteString> {
  require(bufferSize > 0)

  if (!hasRemaining()) {
    return flowOf()
  }
  if (remaining() <= bufferSize) {
    // Optimization.
    return flowOf(toByteString())
  }

  return flow {
    ByteStringOutputBuffer(bufferSize).use { outputBuffer ->
      outputBuffer.putEmittingFull(listOf(this@asBufferedFlow), this)

      // Emit a final value with whatever is left.
      if (outputBuffer.size > 0) {
        emit(outputBuffer.toByteString())
      }
    }
  }
}

/**
 * Creates a flow that produces [ByteString] values with the specified [size][bufferSize] from this
 * [ByteString].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 *
 * This will produce an empty flow if the receiver is empty.
 */
fun ByteString.asBufferedFlow(bufferSize: Int): Flow<ByteString> {
  require(bufferSize > 0)

  if (size == 0) {
    return flowOf()
  }
  if (size <= bufferSize) {
    // Optimization.
    return flowOf(this)
  }

  return flow {
    for (begin in 0 until size() step bufferSize) {
      emit(substring(begin, minOf(size(), begin + bufferSize)))
    }
  }
}

/**
 * Creates a flow that produces [ByteString] values with the specified [size][bufferSize] from this
 * [Flow].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 */
fun Flow<ByteString>.asBufferedFlow(bufferSize: Int): Flow<ByteString> = flow {
  require(bufferSize > 0)

  ByteStringOutputBuffer(bufferSize).use { outputBuffer ->
    collect { outputBuffer.putEmittingFull(it.asReadOnlyByteBufferList(), this) }

    // Emit a final value with whatever is left.
    if (outputBuffer.size > 0) {
      emit(outputBuffer.toByteString())
    }
  }
}

/**
 * Puts all of the remaining bytes from [source] into this buffer, emitting its contents to
 * [collector] and clearing it whenever it gets full.
 */
private suspend fun ByteStringOutputBuffer.putEmittingFull(
  source: Iterable<ByteBuffer>,
  collector: FlowCollector<ByteString>,
) {
  for (buffer in source) {
    while (buffer.hasRemaining()) {
      putUntilFull(buffer)
      if (full) {
        collector.emit(toByteString())
        clear()
      }
    }
  }
}

/**
 * Creates a [Flow] that produces [ByteString] values from this [ReadableByteChannel].
 *
 * @param bufferSize size in bytes of the buffer to use to read from the channel
 */
fun ReadableByteChannel.asFlow(
  bufferSize: Int,
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
): Flow<ByteString> {
  require(bufferSize > 0)

  return flow {
      val buffer = ByteBuffer.allocate(bufferSize)

      while (currentCoroutineContext().isActive) {
        val numRead = read(buffer)
        when {
          numRead < 0 -> {
            emitFrom(buffer)
            return@flow
          }
          numRead == 0 -> {
            // Nothing was read, so we may have a non-blocking channel that nothing can be read
            // from right now. Suspend this coroutine to avoid monopolizing the thread.
            yield()
          }
          else -> {
            if (!buffer.hasRemaining()) {
              emitFrom(buffer)
            }
          }
        }
      }
    }
    .onCompletion { close() }
    .flowOn(coroutineContext)
}

private suspend fun FlowCollector<ByteString>.emitFrom(buffer: ByteBuffer) {
  buffer.flip()
  emit(buffer.toByteString())
  buffer.clear()
}

/**
 * Converts an [InputStream] into a [Flow] of [ByteString]s.
 *
 * This will close the receiver.
 *
 * @param bufferSize size of all except last output ByteString (which may be smaller)
 */
fun InputStream.asFlow(
  bufferSize: Int,
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
): Flow<ByteString> {
  require(bufferSize > 0)

  return flow {
      val buffer = ByteArray(bufferSize)
      while (currentCoroutineContext().isActive) {
        val length = read(buffer)
        if (length < 0) break
        emit(ByteString.copyFrom(buffer, 0, length))
      }
    }
    .onCompletion { close() }
    .flowOn(coroutineContext)
}

/** Reads all of the bytes from this [File] into a [ByteString]. */
fun File.readByteString(): ByteString {
  return inputStream().use { input -> ByteString.readFrom(input) }
}

fun Byte.toStringHex(): String {
  return "%2x".format(this)
}

/** Converts a hex string to its equivalent [ByteString]. */
@Deprecated(
  "Use HexString for stronger typing",
  ReplaceWith("HexString(this).bytes", "org.wfanet.measurement.common.HexString"),
)
fun String.hexAsByteString(): ByteString {
  return HexString(this).bytes
}

/**
 * Reads a varint from a ByteBuffer. Varints are variable-width unsigned integers, supporting up to
 * 64-bit integers.
 *
 * The specification for varints can be found here:
 * https://protobuf.dev/programming-guides/encoding/#varints
 *
 * @return The 64-bit unsigned varint which has been read from the ByteBuffer. This is returned as a
 *   Long.
 */
fun ByteBuffer.getVarInt64(): Long {
  val bytes = Stack<Int>()

  // Load bytes used in the varint
  do {
    val currentByte = this.get().toInt()

    // Use a stack to store bytes so that the order is implicitly in big-endian when reading.
    bytes.push(currentByte)

    // If the current byte's MSB is 0, it is the final bit in the varint.
    // Otherwise, keep reading.
  } while (currentByte and 0x80 != 0)

  var result = 0L

  while (!bytes.empty()) {
    // Move anything currently in the result 7 bits to the left to make way for new bits.
    result = result shl 7

    // Read the next bit from the stack
    var currentByte = bytes.pop()

    // Add the least-significant 7 bits to the result
    currentByte = currentByte and 0x7f
    result = result or currentByte.toLong()
  }

  return result
}

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
  ReplaceWith("toByteString()", "com.google.protobuf.kotlin.toByteString")
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

  return this.concat(ByteString.newOutput(paddedSize - size)
    .use { output ->
      repeat(paddedSize - size) { output.write(0x00) }
      output.toByteString()
    })
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
  collector: FlowCollector<ByteString>
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
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO
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
  coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO
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
  ReplaceWith("HexString(this).bytes", "org.wfanet.measurement.common.HexString")
)
fun String.hexAsByteString(): ByteString {
  return HexString(this).bytes
}

/**
 * Converts a bytearray representing a little-endian, 64-bit number into a long.
 *
 * @param byteArray A ByteArray with 8 elements which represents a little-endian, 64-bit number.
 * @return A long representation of the provided ByteArray.
 * @throws IndexOutOfBoundsException if the size of the provided ByteArray is not 8.
 */
fun readLittleEndian64Long(byteString: ByteString): Long {
  if (byteString.size() != 8) {
    throw IndexOutOfBoundsException("Byte array provided is not the correct length")
  }

  val byteArray = byteString.toByteArray()

  return ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).long
}

/**
 * Converts a bytearray representing a little-endian, 64-bit number into an int.
 *
 * @param byteArray A ByteArray with 8 elements which represents a little-endian, 64-bit number.
 * @return An integer representation of the provided ByteArray.
 * @throws IndexOutOfBoundsException if the size of the provided ByteArray is not 8.
 */
fun readLittleEndian64Int(byteString: ByteString): Int {
  return readLittleEndian64Long(byteString).toInt()
}

/**
 * Converts a bytearray representing a little-endian, 56-bit number into an int.
 *
 * @param byteArray A ByteArray with 7 elements which represents a little-endian, 56-bit number.
 * @return An integer representation of the provided ByteArray.
 * @throws IndexOutOfBoundsException if the size of the provided ByteArray is not 7.
 */
fun readLittleEndian56Int(byteString: ByteString): Int {
  if (byteString.size() != 7) {
    throw IndexOutOfBoundsException("Byte array provided is not the correct length")
  }

  return readLittleEndian64Int(byteString.withTrailingPadding(8))
}

/**
 * Reads a varint from an InputStream.
 * Varints are variable-width integers.
 *
 * Code snippet taken from
 *   https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/VarInt.java
 *
 * @param inputStream The input stream from which the varint should be read.
 * @return The varint which has been read from the input stream.
 */
fun getVarInt(byteBuffer: ByteBuffer): Int {
  var result = 0
  var shift = 0
  var b: Int
  do {
    if (shift >= 32) {
      // Out of range
      throw IndexOutOfBoundsException("varint too long")
    }
    // Get 7 bits from next byte
    b = byteBuffer.get().toInt()
    result = result or (b and 0x7F shl shift)
    shift += 7
  } while (b and 0x80 != 0)
  return result
}

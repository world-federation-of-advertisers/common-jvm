// Copyright 2024 The Cross-Media Measurement Authors
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
import java.nio.channels.ReadableByteChannel
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * A non-blocking [ReadableByteChannel] that reads data from a [ReceiveChannel] of [ByteString].
 * This class enables coroutine-friendly, asynchronous reads by delegating read operations to a
 * coroutine channel.
 *
 * @property delegate The [ReceiveChannel] from which this [ReadableByteChannel] will read data.
 * @constructor Creates a readable channel that reads each [ByteString] from the provided
 *   [ReceiveChannel] and writes it to the specified [ByteBuffer].
 */
class CoroutineReadableByteChannel(private val delegate: ReceiveChannel<ByteString>) :
  ReadableByteChannel {

  private var remainingBytes: ByteArray? = null
  private var remainingOffset = 0

  /**
   * Reads bytes from the [ReceiveChannel] and transfers them into the provided buffer. If there are
   * leftover bytes from a previous read that didn’t fit in the buffer, they are written first.
   *
   * When the [ReceiveChannel] has no data available:
   * - Returns `0` if the channel is open but temporarily empty, allowing the caller to retry later.
   * - Returns `-1` if the channel is closed and no more data will arrive.
   *
   * If only part of a [ByteString] fits into `destination`, the unread portion is saved for future
   * reads, ensuring data is preserved between calls.
   *
   * @param destination The [ByteBuffer] where data will be written.
   * @return The number of bytes written to `destination`, `0` if no data is available, or `-1` if
   *   the channel is closed and all data has been read.
   */
  override fun read(destination: ByteBuffer): Int {
    remainingBytes?.let {
      val bytesToWrite = minOf(destination.remaining(), it.size - remainingOffset)
      destination.put(it, remainingOffset, bytesToWrite)
      remainingOffset += bytesToWrite

      if (remainingOffset >= it.size) {
        remainingBytes = null
        remainingOffset = 0
      }
      return bytesToWrite
    }

    val result = delegate.tryReceive()
    val byteString = result.getOrNull() ?: return if (result.isClosed) -1 else 0
    val bytesToWrite = minOf(destination.remaining(), byteString.size())
    byteString.substring(0, bytesToWrite).copyTo(destination)
    if (bytesToWrite < byteString.size()) {
      remainingBytes = byteString.toByteArray()
      remainingOffset = bytesToWrite
    }

    return bytesToWrite
  }

  override fun isOpen(): Boolean = !delegate.isClosedForReceive

  override fun close() {
    delegate.cancel()
  }
}

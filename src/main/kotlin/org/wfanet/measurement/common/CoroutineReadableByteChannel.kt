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
import java.nio.channels.ClosedChannelException
import java.nio.channels.ReadableByteChannel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * A non-blocking [ReadableByteChannel] that reads data from a [ReceiveChannel] of [ByteString].
 * This class enables coroutine-friendly, asynchronous reads by delegating read operations to a
 * coroutine channel.
 *
 * @property delegate The [ReceiveChannel] from which this [ReadableByteChannel] will read data.
 */
class CoroutineReadableByteChannel(private val delegate: ReceiveChannel<ByteString>) :
  ReadableByteChannel {

  private var closed = false

  /** Buffer of bytes read from [delegate] but not yet read by the caller. */
  private var sourceBuffer: ByteBuffer = ByteString.EMPTY.asReadOnlyByteBuffer()

  override fun read(destination: ByteBuffer): Int {
    if (closed) {
      throw ClosedChannelException()
    }

    var writtenCountBytes = 0
    while (destination.hasRemaining()) {
      // Read from delegate channel if source buffer is empty.
      if (!sourceBuffer.hasRemaining()) {
        val result: ChannelResult<ByteString> = delegate.tryReceive()
        if (result.isFailure) {
          if (result.isClosed) {
            if (writtenCountBytes == 0) {
              return -1
            }
          }
          return writtenCountBytes
        }

        val source: ByteString = result.getOrThrow()
        sourceBuffer = source.asReadOnlyByteBuffer()
      }

      // Put into destination buffer.
      if (sourceBuffer.hasRemaining()) {
        writtenCountBytes += destination.tryPut(sourceBuffer)
      }
    }

    return writtenCountBytes
  }

  override fun isOpen(): Boolean = !closed

  override fun close() {
    if (closed) {
      return
    }

    delegate.cancel()
    closed = true
  }
}

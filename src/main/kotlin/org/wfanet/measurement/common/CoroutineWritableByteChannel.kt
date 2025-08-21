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
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.WritableByteChannel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking

/**
 * A [WritableByteChannel] that writes data to [delegate].
 *
 * This class enables coroutine-friendly, asynchronous writes by delegating write operations to a
 * coroutine channel.
 */
class CoroutineWritableByteChannel
private constructor(private val delegate: SendChannel<ByteString>, private val blocking: Boolean) :
  WritableByteChannel {
  private var closed = false

  override fun write(src: ByteBuffer): Int {
    if (closed) {
      throw ClosedChannelException()
    }

    val slice: ByteBuffer = src.slice()
    val chunk = ByteString.copyFrom(slice)
    val result: ChannelResult<Unit> =
      if (blocking) {
        delegate.trySendBlocking(chunk)
      } else {
        delegate.trySend(chunk)
      }
    if (result.isClosed) {
      throw IOException("delegate channel is closed")
    } else if (result.isFailure) {
      return 0
    }

    src.position(src.position() + chunk.size)
    return chunk.size
  }

  override fun isOpen(): Boolean = !closed

  override fun close() {
    if (closed) {
      return
    }

    delegate.close()
    closed = true
  }

  companion object {
    /**
     * Creates a [CoroutineWritableByteChannel] in blocking mode.
     *
     * This means that the [write][CoroutineWritableByteChannel.write] method will block until all
     * bytes can be written.
     */
    fun createBlocking(delegate: SendChannel<ByteString>) =
      CoroutineWritableByteChannel(delegate, true)

    /**
     * Creates a [CoroutineWritableByteChannel] in non-blocking mode.
     *
     * This means that the [write][CoroutineWritableByteChannel.write] method will never block, but
     * may not write all bytes if [delegate] isn't able to receive.
     */
    fun createNonBlocking(delegate: SendChannel<ByteString>) =
      CoroutineWritableByteChannel(delegate, false)
  }
}

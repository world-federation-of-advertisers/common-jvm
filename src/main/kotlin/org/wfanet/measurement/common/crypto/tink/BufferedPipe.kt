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

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.wfanet.measurement.common.tryPut

/**
 * Similar to [java.nio.channels.Pipe], but with a buffer of known [capacity].
 *
 * The [write][java.nio.channels.WritableByteChannel.write] method on [sink] should not block while
 * there is enough remaining space in the buffer to complete the write, assuming there is no
 * concurrent reader.
 */
class BufferedPipe(capacity: Int) : AutoCloseable {
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

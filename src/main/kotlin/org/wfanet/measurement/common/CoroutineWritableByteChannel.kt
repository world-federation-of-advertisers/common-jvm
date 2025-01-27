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
import java.nio.channels.WritableByteChannel
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.channels.SendChannel

/**
 * A non-blocking [WritableByteChannel] that writes data to a [SendChannel] of [ByteString]. This
 * class enables coroutine-friendly, asynchronous writes by delegating write operations to a
 * coroutine channel.
 *
 * @property delegate The [SendChannel] to which this [WritableByteChannel] will send data.
 * @constructor Creates a writable channel that writes each [ByteBuffer] as a [ByteString] to the
 *   provided [SendChannel].
 */
class CoroutineWritableByteChannel(private val delegate: SendChannel<ByteString>) :
  WritableByteChannel {

  /**
   * Writes the contents of the provided [ByteBuffer] to the [SendChannel] as a [ByteString].
   *
   * If the channel is not ready to accept data, the method returns `0` without consuming any bytes
   * from the buffer, allowing the caller to retry. If the channel is closed, a
   * [ClosedChannelException] is thrown.
   *
   * @param source The [ByteBuffer] containing the data to write.
   * @return The number of bytes written, or `0` if the channel is temporarily unable to accept
   *   data.
   * @throws ClosedChannelException if the channel is closed and cannot accept more data.
   */
  override fun write(source: ByteBuffer): Int {
    val originalPosition = source.position()
    val bytesToWrite = source.remaining()
    val byteString = ByteString.copyFrom(source)
    val result = delegate.trySend(byteString)
    if (result.isClosed) {
      throw ClosedChannelException()
    } else if (result.isFailure) {
      source.position(originalPosition)
      return 0
    }
    return bytesToWrite
  }

  @OptIn(DelicateCoroutinesApi::class) // Safe usage since write is guarded by trySend.
  override fun isOpen(): Boolean = !delegate.isClosedForSend

  override fun close() {
    delegate.close()
  }
}

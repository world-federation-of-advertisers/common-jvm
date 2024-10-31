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
import kotlinx.coroutines.channels.SendChannel

/**
 * A non-blocking [WritableByteChannel] that writes data to a [SendChannel] of [ByteString]. This
 * class enables coroutine-friendly, asynchronous writes by delegating write operations to a
 * coroutine channel.
 *
 * @property delegate The [SendChannel] to which this [WritableByteChannel] will send data.
 * @constructor Creates a writable channel that writes each [ByteBuffer] as a [ByteString] to the
 *   provided [SendChannel].
 * @throws ClosedChannelException if the channel is closed and cannot accept more data.
 */
class CoroutineWritableByteChannel(private val delegate: SendChannel<ByteString>) :
  WritableByteChannel {
  override fun write(source: ByteBuffer): Int {
    val byteString = ByteString.copyFrom(source)
    if (delegate.trySend(byteString).isClosed) {
      throw ClosedChannelException()
    }
    return byteString.size()
  }

  override fun isOpen(): Boolean = !delegate.isClosedForSend

  override fun close() {
    delegate.close()
  }
}

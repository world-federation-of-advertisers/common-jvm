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
 * @return The number of bytes read into the buffer, or -1 if the channel is closed and no data
 *   remains.
 * @constructor Creates a readable channel that reads each [ByteString] from the provided
 *   [ReceiveChannel] and writes it to the specified [ByteBuffer].
 */
class CoroutineReadableByteChannel(private val delegate: ReceiveChannel<ByteString>) :
  ReadableByteChannel {
  override fun read(destination: ByteBuffer): Int {
    val result = delegate.tryReceive()
    val byteString = result.getOrNull() ?: return -1 // -1 indicates the end of the stream
    destination.put(byteString.toByteArray())
    return byteString.size()
  }

  override fun isOpen(): Boolean = !delegate.isClosedForReceive

  override fun close() {
    delegate.cancel()
  }
}

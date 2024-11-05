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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class CoroutineReadableByteChannelTest {

  @Test
  fun `read - reads data from channel`() = runBlocking {
    val testData = ByteString.copyFromUtf8("hello")

    val channel = Channel<ByteString>()
    val coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)
    val buffer = ByteBuffer.allocate(5)
    launch {
      val bytesRead = coroutineReadableByteChannel.read(buffer)
      assertThat(bytesRead).isEqualTo(testData.size())
    }
    channel.send(testData)
    assertThat(buffer.array().toList()).isEqualTo(testData.toByteArray().toList())

    coroutineReadableByteChannel.close()
  }

  @Test
  fun `read buffers channel items when destination buffer is filled`() = runBlocking {
    val testData = ByteString.copyFromUtf8("hello world")

    val channel = Channel<ByteString>()
    val coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)

    val buffer = ByteBuffer.allocate(5)

    launch {
      val bytesRead = coroutineReadableByteChannel.read(buffer)
      assertThat(bytesRead).isEqualTo(5)
      buffer.flip()
      assertThat(Charsets.UTF_8.decode(buffer).toString()).isEqualTo("hello")
      buffer.clear()
      var remainingBytesRead = coroutineReadableByteChannel.read(buffer)
      buffer.flip()
      assertThat(Charsets.UTF_8.decode(buffer).toString()).isEqualTo(" worl")
      assertThat(remainingBytesRead).isEqualTo(5)
      buffer.clear()
      remainingBytesRead = coroutineReadableByteChannel.read(buffer)
      buffer.flip()
      assertThat(Charsets.UTF_8.decode(buffer).toString()).isEqualTo("d")
      assertThat(remainingBytesRead).isEqualTo(1)
    }

    channel.send(testData)
    coroutineReadableByteChannel.close()
  }

  @Test
  fun `read - returns 0 when no data available`() = runBlocking {
    val channel = Channel<ByteString>()
    val coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)

    val buffer = ByteBuffer.allocate(5)
    val bytesRead = coroutineReadableByteChannel.read(buffer)
    assertThat(bytesRead).isEqualTo(0)

    coroutineReadableByteChannel.close()
  }

  @Test
  fun `read - returns -1 when channel is closed and empty`() = runBlocking {
    val channel = Channel<ByteString>()
    val coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)
    channel.close()
    val buffer = ByteBuffer.allocate(5)
    val bytesRead = coroutineReadableByteChannel.read(buffer)
    assertThat(bytesRead).isEqualTo(-1)
  }

  @Test
  fun `isOpen - returns false after close`() = runBlocking {
    val channel = Channel<ByteString>()
    val coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)
    coroutineReadableByteChannel.close()
    assertThat(coroutineReadableByteChannel.isOpen()).isFalse()
  }

  @Test
  fun `isOpen - returns true when channel is open`() = runBlocking {
    val channel = Channel<ByteString>()
    val coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)
    assertThat(coroutineReadableByteChannel.isOpen()).isTrue()

    coroutineReadableByteChannel.close()
  }
}

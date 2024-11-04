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
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class CoroutineReadableByteChannelTest {

  private lateinit var channel: Channel<ByteString>
  private lateinit var coroutineReadableByteChannel: CoroutineReadableByteChannel

  @Before
  fun setUp() {
    channel = Channel()
    coroutineReadableByteChannel = CoroutineReadableByteChannel(channel)
  }

  @After
  fun tearDown() {
    coroutineReadableByteChannel.close()
  }

  @Test
  fun `read - reads data from channel`() = runBlocking {
    val testData = ByteString.copyFromUtf8("hello")
    val buffer = ByteBuffer.allocate(5)
    launch { channel.send(testData) }
    delay(100)
    val bytesRead = coroutineReadableByteChannel.read(buffer)
    assertThat(testData.size()).isEqualTo(bytesRead)
    assertThat(testData.toByteArray().toList()).isEqualTo(buffer.array().toList())
  }

  @Test
  fun `read - handles remaining bytes correctly`() = runBlocking {
    val testData = ByteString.copyFromUtf8("hello world")
    val buffer = ByteBuffer.allocate(5)
    launch { channel.send(testData) }
    delay(100)
    val bytesRead = coroutineReadableByteChannel.read(buffer)
    assertThat(5).isEqualTo(bytesRead)
    buffer.flip()
    assertThat("hello").isEqualTo(ByteString.copyFrom(buffer).toStringUtf8())
    buffer.clear()
    var remainingBytesRead = coroutineReadableByteChannel.read(buffer)
    buffer.flip()
    assertThat(" worl").isEqualTo(ByteString.copyFrom(buffer).toStringUtf8())
    assertThat(5).isEqualTo(remainingBytesRead)
    buffer.clear()
    remainingBytesRead = coroutineReadableByteChannel.read(buffer)
    buffer.flip()
    assertThat("d").isEqualTo(ByteString.copyFrom(buffer).toStringUtf8())
    assertThat(1).isEqualTo(remainingBytesRead)
  }

  @Test
  fun `read - returns 0 when no data available`() = runBlocking {
    val buffer = ByteBuffer.allocate(5)
    val bytesRead = coroutineReadableByteChannel.read(buffer)
    assertThat(0).isEqualTo(bytesRead)
  }

  @Test
  fun `read - returns -1 when channel is closed and empty`() = runBlocking {
    channel.close()
    val buffer = ByteBuffer.allocate(5)
    val bytesRead = coroutineReadableByteChannel.read(buffer)
    assertThat(-1).isEqualTo(bytesRead)
  }

  @Test
  fun `isOpen - returns false after close`() = runBlocking {
    coroutineReadableByteChannel.close()
    assertThat(coroutineReadableByteChannel.isOpen()).isFalse()
  }

  @Test
  fun `isOpen - returns true when channel is open`() = runBlocking {
    assertThat(coroutineReadableByteChannel.isOpen()).isTrue()
  }
}

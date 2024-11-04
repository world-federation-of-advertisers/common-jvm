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
import java.nio.channels.ClosedChannelException
import kotlin.test.assertFailsWith
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
class CoroutineWritableByteChannelTest {

  private lateinit var channel: Channel<ByteString>
  private lateinit var coroutineWritableByteChannel: CoroutineWritableByteChannel

  @Before
  fun setUp() {
    channel = Channel()
    coroutineWritableByteChannel = CoroutineWritableByteChannel(channel)
  }

  @After
  fun tearDown() {
    coroutineWritableByteChannel.close()
  }

  @Test
  fun `write - successfully writes data to channel`() = runBlocking {
    val testData = "hello world"
    val buffer = ByteBuffer.wrap(testData.toByteArray())
    val receivedData = launch {
      val received = channel.receive()
      assertThat(received.toStringUtf8()).isEqualTo(testData)
    }
    delay(100)
    val bytesWritten = coroutineWritableByteChannel.write(buffer)
    assertThat(bytesWritten).isEqualTo(testData.length)
    receivedData.join()
  }

  @Test
  fun `write - returns zero when no receiver available`(): Unit = runBlocking {
    val testData = "test data"
    val buffer = ByteBuffer.wrap(testData.toByteArray())
    val bytesWritten = coroutineWritableByteChannel.write(buffer)
    assertThat(bytesWritten).isEqualTo(0)
    assertThat(buffer.position()).isEqualTo(0)
  }

  @Test
  fun `write - throws when channel is closed`(): Unit = runBlocking {
    val testData = "test data"
    val buffer = ByteBuffer.wrap(testData.toByteArray())

    channel.close()

    assertFailsWith(ClosedChannelException::class) { coroutineWritableByteChannel.write(buffer) }
  }

  @Test
  fun `isOpen - returns false when channel is closed`() = runBlocking {
    assertThat(coroutineWritableByteChannel.isOpen()).isTrue()

    channel.close()

    assertThat(coroutineWritableByteChannel.isOpen()).isFalse()
  }

  @Test
  fun `write - handles large data correctly`() = runBlocking {
    val testData = ByteArray(1024) { it.toByte() }
    val buffer = ByteBuffer.wrap(testData)
    val receivedData = launch {
      val received = channel.receive()
      assertThat(received.toByteArray()).isEqualTo(testData)
    }
    delay(100)
    val bytesWritten = coroutineWritableByteChannel.write(buffer)
    assertThat(bytesWritten).isEqualTo(testData.size)
    receivedData.join()
  }

  @Test
  fun `write - maintains buffer position`() = runBlocking {
    val testData = "hello world"
    val buffer = ByteBuffer.wrap(testData.toByteArray())
    buffer.position(6)
    val receivedData = launch {
      val received = channel.receive()
      assertThat(received.toStringUtf8()).isEqualTo("world")
    }
    delay(100)
    val bytesWritten = coroutineWritableByteChannel.write(buffer)
    assertThat(bytesWritten).isEqualTo(5)
    assertThat(buffer.position()).isEqualTo(11)
    receivedData.join()
  }
}

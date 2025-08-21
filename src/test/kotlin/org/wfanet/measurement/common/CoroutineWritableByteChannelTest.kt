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
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class CoroutineWritableByteChannelTest {

  @Test
  fun `write writes data to channel`() = runBlocking {
    val testData = "hello world"

    val channel = Channel<ByteString>()
    CoroutineWritableByteChannel.createNonBlocking(channel).use { coroutineWritableByteChannel ->
      val buffer = ByteBuffer.wrap(testData.toByteArray())
      launch {
        val bytesWritten = coroutineWritableByteChannel.write(buffer)
        assertThat(bytesWritten).isEqualTo(testData.length)
      }

      val received = channel.receive()
      assertThat(received.toStringUtf8()).isEqualTo(testData)
    }
  }

  @Test
  fun `write returns zero when delegate channel buffer is full`(): Unit = runBlocking {
    val testData = "test data"
    val channel = Channel<ByteString>()
    CoroutineWritableByteChannel.createNonBlocking(channel).use { coroutineWritableByteChannel ->
      val buffer = ByteBuffer.wrap(testData.toByteArray())

      val bytesWritten = coroutineWritableByteChannel.write(buffer)

      assertThat(bytesWritten).isEqualTo(0)
      assertThat(buffer.position()).isEqualTo(0)
    }
  }

  @Test
  fun `write throws when delegate channel is closed`(): Unit = runBlocking {
    val testData = "test data"
    val channel = Channel<ByteString>()
    CoroutineWritableByteChannel.createNonBlocking(channel).use { coroutineWritableByteChannel ->
      val buffer = ByteBuffer.wrap(testData.toByteArray())

      channel.close()

      val exception = assertFailsWith<IOException> { coroutineWritableByteChannel.write(buffer) }
      assertThat(exception).hasMessageThat().contains("closed")
    }
  }

  @Test
  fun `write throws when closed`(): Unit = runBlocking {
    val testData = "test data"
    val sourceBuffer = ByteBuffer.wrap(testData.toByteArray())
    val channel = Channel<ByteString>()
    val coroutineWritableByteChannel = CoroutineWritableByteChannel.createNonBlocking(channel)
    coroutineWritableByteChannel.close()

    assertFailsWith<ClosedChannelException> { coroutineWritableByteChannel.write(sourceBuffer) }
  }

  @Test
  fun `write handles large data correctly`() = runBlocking {
    val channel = Channel<ByteString>()
    val coroutineWritableByteChannel = CoroutineWritableByteChannel.createNonBlocking(channel)
    val testData = ByteArray(1024) { it.toByte() }
    val buffer = ByteBuffer.wrap(testData)
    launch {
      val bytesWritten = coroutineWritableByteChannel.write(buffer)
      assertThat(bytesWritten).isEqualTo(testData.size)
    }
    val received = channel.receive()
    assertThat(received.toByteArray()).isEqualTo(testData)

    coroutineWritableByteChannel.close()
  }

  @Test
  fun `write maintains buffer position`() = runBlocking {
    val testData = "hello world"
    val channel = Channel<ByteString>()
    val buffer = ByteBuffer.wrap(testData.toByteArray())
    buffer.position(6)

    CoroutineWritableByteChannel.createNonBlocking(channel).use { coroutineWritableByteChannel ->
      launch {
        val bytesWritten = coroutineWritableByteChannel.write(buffer)
        assertThat(bytesWritten).isEqualTo(5)
        assertThat(buffer.position()).isEqualTo(11)
      }

      val received = channel.receive()
      assertThat(received.toStringUtf8()).isEqualTo("world")
    }
  }
}

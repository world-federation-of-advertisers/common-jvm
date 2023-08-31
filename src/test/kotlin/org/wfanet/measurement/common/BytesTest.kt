// Copyright 2020 The Cross-Media Measurement Authors
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
import java.nio.ByteOrder
import kotlin.test.assertFails
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class BytesTest {
  @Test
  fun `ByteString is bidirectionally convertible with Long`() {
    val longValue = -3519155157501101422L
    val binaryValue = HexString("CF2973078FA9BA92").bytes

    assertThat(longValue.toByteString()).isEqualTo(binaryValue)
    assertThat(binaryValue.toLong()).isEqualTo(longValue)
  }

  @Test
  fun `ByteString is bidirectionally convertible with Long using little endian`() {
    val longValue = -3519155157501101422L
    val binaryValue = HexString("92BAA98F077329CF").bytes

    assertThat(longValue.toByteString(ByteOrder.LITTLE_ENDIAN)).isEqualTo(binaryValue)
    assertThat(binaryValue.toLong(ByteOrder.LITTLE_ENDIAN)).isEqualTo(longValue)
  }

  @Test
  fun `ByteString asBufferedFlow with non-full last part`() = runBlocking {
    val flow = ByteString.copyFromUtf8("Hello World").asBufferedFlow(3)
    assertThat(flow.map { it.toStringUtf8() }.toList())
      .containsExactly("Hel", "lo ", "Wor", "ld")
      .inOrder()
  }

  @Test
  fun `ByteString asBufferedFlow on empty ByteString`() = runBlocking {
    val flow = ByteString.copyFromUtf8("").asBufferedFlow(3)
    assertThat(flow.toList()).isEmpty()
  }

  @Test
  fun `ByteString asBufferedFlow with invalid buffer size`(): Unit = runBlocking {
    assertFails { ByteString.copyFromUtf8("this should throw").asBufferedFlow(0).toList() }
  }

  @Test
  fun `ByteString with trailing padding on empty ByteString`() {
    val byteString = ByteString.EMPTY
    val byteStringWithPadding = byteString.withTrailingPadding(8)

    assertThat(byteStringWithPadding.size()).isEqualTo(8)
    byteString.forEach { byte -> assertThat(byte).isEqualTo(0x00.toByte()) }
  }

  @Test
  fun `ByteString with trailing padding on normal ByteString`() {
    val byteString = ByteString.copyFrom(byteArrayOf(0x96.toByte(), 0x01.toByte()))
    val byteStringWithPadding = byteString.withTrailingPadding(4)

    assertThat(byteStringWithPadding.size()).isEqualTo(4)

    val comparableByteArray =
      byteArrayOf(0x96.toByte(), 0x01.toByte(), 0x00.toByte(), 0x00.toByte())

    byteString.forEachIndexed { index, byte ->
      assertThat(byte).isEqualTo(comparableByteArray[index])
    }
  }

  @Test
  fun `Read 64 bit varint from ByteBuffer`() {
    // [10010110, 00000001]
    val bytes = byteArrayOf(0x96.toByte(), 0x01.toByte())
    val byteBuffer = ByteBuffer.wrap(bytes)

    assertThat(byteBuffer.getVarInt64()).isEqualTo(150L)
  }
}

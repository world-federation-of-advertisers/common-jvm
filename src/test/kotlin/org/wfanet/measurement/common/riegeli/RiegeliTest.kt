// Copyright 2023 The Cross-Media Measurement Authors
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
// limitations under the \License.

package org.wfanet.measurement.common.riegeli

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.io.File
import java.nio.file.Paths
import kotlin.math.min
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.testing.riegeli.SimpleMessage

@RunWith(JUnit4::class)
class RiegeliTest {

  fun sampleString(i: Int, size: Int): ByteString {
    val piece = "$i ".encodeToByteArray()
    val result = ByteArray(size)
    var j = 0
    while (j < size) {
      System.arraycopy(piece, 0, result, j, min(piece.size, size - j))
      j += piece.size
    }
    return ByteString.copyFrom(result.copyOf(size))
  }

  @Test
  fun `readCompressedFilesWithRecords returns records`() {
    val records = Riegeli.readRecords(SIMPLE_MESSAGE_RIEGELI_FILE)

    records.forEachIndexed { index: Int, it ->
      val simpleMessage = SimpleMessage.parseFrom(it)
      assertThat(simpleMessage.id).isEqualTo(index)
      assertThat(simpleMessage.payload).isEqualTo(sampleString(index, 10000))
    }
  }

  @Test
  fun `readCompressedInputStreamWithRecords returns records`() {
    Riegeli.readRecords(SIMPLE_MESSAGE_RIEGELI_FILE.inputStream()).forEachIndexed {
      index: Int,
      messageBytes: ByteString ->
      val messageObject = SimpleMessage.parseFrom(messageBytes)
      assertThat(messageObject.id).isEqualTo(index)
      assertThat(messageObject.payload).isEqualTo(sampleString(index, 10000))
    }
  }

  @Test
  fun `readCompressedFileWithRecords can be collected`() {
    val records = Riegeli.readRecords(SIMPLE_MESSAGE_RIEGELI_FILE)

    val simpleMessages = records.map { SimpleMessage.parseFrom(it).payload }

    val comparisonArray = List(23) { index -> sampleString(index, 10000) }

    assertThat(simpleMessages.toList()).isEqualTo(comparisonArray)
  }

  @Test
  fun `readCompressedInputStreamWithRecords can be collected`() {
    val records = Riegeli.readRecords(SIMPLE_MESSAGE_RIEGELI_FILE.inputStream())

    val simpleMessages = records.map { SimpleMessage.parseFrom(it).payload }

    val comparisonArray = List(23) { index -> sampleString(index, 10000) }

    assertThat(simpleMessages.toList()).isEqualTo(comparisonArray)
  }

  @Test
  fun `readCompressedFileWithRecords with corrupted file throws exception`() {
    assertFailsWith<Exception> {
      // Need to call toList in order to get the reader to go through the file
      Riegeli.readRecords(CORRUPTED_MESSAGE_RIEGELI_FILE).toList()
    }
  }

  companion object {
    private val FIXED_TESTDATA_DIR: File =
      getRuntimePath(
          Paths.get(
            "wfa_common_jvm",
            "src",
            "main",
            "kotlin",
            "org",
            "wfanet",
            "measurement",
            "common",
            "riegeli",
            "testing"
          )
        )!!
        .toFile()

    private val SIMPLE_MESSAGE_RIEGELI_FILE = FIXED_TESTDATA_DIR.resolve("simple_message_riegeli")
    private val CORRUPTED_MESSAGE_RIEGELI_FILE =
      FIXED_TESTDATA_DIR.resolve("corrupted_message_riegeli")
  }
}

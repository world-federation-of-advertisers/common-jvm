// Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import java.io.FileInputStream
import java.io.InputStream
import kotlin.math.min
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import riegeli.tests.SimpleMessageOuterClass.SimpleMessage

private const val DATA_PATH = "src/test/proto/wfa/measurement/common/riegeli/test_data"

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
  fun `test read record from file` () {
    val filename = "$DATA_PATH/simple_message_riegeli"
    val records = Riegeli().readCompressedFileWithRecords(filename)

    val simpleMessages = records.map {
      SimpleMessage.parseFrom(it)
    }.toTypedArray()

    simpleMessages.forEachIndexed { index: Int, simpleMessage: SimpleMessage ->
      assertEquals(simpleMessage.id, index)
      assert(simpleMessage.payload.equals(sampleString(index, 10000)))
    }
  }

  @Test
  fun `test read record from input stream` () {
    val filename = "$DATA_PATH/simple_message_riegeli"
    val fileInputStream = FileInputStream(filename)

    Riegeli()
      .readCompressedInputStreamWithRecords(fileInputStream)
      .forEachIndexed { index: Int, messageBytes: ByteString ->
        val messageObject = SimpleMessage.parseFrom(messageBytes)
        assertEquals(messageObject.id, index)
        assert(messageObject.payload.equals(sampleString(index, 10000)))
      }
  }

  @Test
  fun `test read records` () {
    val filename = "$DATA_PATH/simple_message_riegeli"
    val records = Riegeli().readCompressedFileWithRecords(filename)

    val simpleMessages = records.map {
      SimpleMessage.parseFrom(it).payload
    }.toTypedArray()

    val comparisonArray = Array(23) {index -> sampleString(index, 10000)}

    assertContentEquals(simpleMessages, comparisonArray)
  }

  @Test
  fun `test_invalid_records_exception` () {
    val filename = "$DATA_PATH/corrupted_message_riegeli"
    val fileInputStream = FileInputStream(filename)

    assertFailsWith<Exception> {
      Riegeli().readCompressedInputStreamWithRecords(fileInputStream)
    }
  }
}

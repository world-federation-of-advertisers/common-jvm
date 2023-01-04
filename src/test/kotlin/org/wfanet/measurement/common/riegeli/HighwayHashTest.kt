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

package org.wfanet.measurement.common.riegeli

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class HighwayHashTest {

  @Test
  fun `test highwayhash`() {
    val highwayHash = HighwayHash(0x2f696c6567656952, 0x0a7364726f636572, 0x2f696c6567656952, 0x0a7364726f636572)


    val hash = arrayOf(
      0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9,
    ).map { it.toByte() }.toByteArray()

    val hashLong = ByteBuffer.wrap(hash).order(ByteOrder.LITTLE_ENDIAN).getLong()

    val byteArray = arrayOf(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00).map { it.toByte() }.toByteArray()


    highwayHash.updatePacket(byteArray, 0)

    assertEquals(highwayHash.finalize64(), hashLong)
  }
}

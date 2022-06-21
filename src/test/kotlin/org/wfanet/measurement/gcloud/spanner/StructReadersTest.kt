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

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.Timestamp as CloudTimestamp
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class StructReadersTest {
  private val timestamp = CloudTimestamp.now()
  private val str = "abcdefg"
  private val strArray: List<String> = (1..5).map { "str$it" }
  private val int64 = 405060708090100L
  private val struct = struct {
    set("nullString").to(null as String?)
    set("stringValue").to(str)
    set("nullInt64").to(null as Long?)
    set("int64Value").to(int64)
    set("nullTimestamp").to(null as CloudTimestamp?)
    set("timestampValue").to(timestamp)
    set("stringArrayValue").toStringArray(strArray)
  }

  @Test
  fun `get returns value`() {
    val stringValue: String = struct["stringValue"]
    val int64Value: Long = struct["int64Value"]
    val timestampValue: CloudTimestamp = struct["timestampValue"]
    val stringArrayValue: List<String> = struct["stringArrayValue"]

    assertThat(stringValue).isEqualTo(str)
    assertThat(int64Value).isEqualTo(int64)
    assertThat(timestampValue).isEqualTo(timestamp)
    assertThat(stringArrayValue).containsExactlyElementsIn(strArray).inOrder()
  }

  @Test
  fun `get returns null`() {
    val nullString: String? = struct["nullString"]
    val nullInt64: Long? = struct["nullInt64"]
    val nullTimestamp: CloudTimestamp? = struct["nullTimestamp"]

    assertThat(nullString).isNull()
    assertThat(nullInt64).isNull()
    assertThat(nullTimestamp).isNull()
  }

  @Test
  fun `get can return nullable type for non-null value`() {
    val nullableString: String? = struct["stringValue"]
    assertThat(nullableString).isEqualTo(str)
  }

  @Test
  fun `get throws for null value with non-nullable type`() {
    val exception = assertFailsWith<IllegalArgumentException> { struct.get<String>("nullString") }
    assertThat(exception).hasMessageThat().ignoringCase().contains("nullable")
  }

  @Test
  fun `get throws for incorrect type`() {
    val exception = assertFailsWith<IllegalStateException> { struct.get<Long>("stringValue") }
    assertThat(exception).hasMessageThat().ignoringCase().contains("type")
  }

  @Test
  fun `get throws for incorrect array type`() {
    val exception =
      assertFailsWith<IllegalStateException> { struct.get<List<Long>>("stringArrayValue") }
    assertThat(exception).hasMessageThat().ignoringCase().contains("type")
  }

  @Test
  fun `get throws for non-existent column`() {
    val exception = assertFailsWith<IllegalArgumentException> { struct.get<String>("column404") }
    assertThat(exception).hasMessageThat().ignoringCase().contains("not found")
  }

  @Test
  fun getNullableString() {
    assertNull(struct.getNullableString("nullString"))
    assertEquals(str, struct.getNullableString("stringValue"))
    assertFailsWith<IllegalStateException> { struct.getNullableString("timestampValue") }
  }

  @Test
  fun getNullableInt64() {
    assertNull(struct.getNullableLong("nullInt64"))
    assertEquals(int64, struct.getNullableLong("int64Value"))
    assertFailsWith<IllegalStateException> { struct.getNullableLong("timestampValue") }
  }

  @Test
  fun getNullableTimestamp() {
    assertNull(struct.getNullableTimestamp("nullTimestamp"))
    assertEquals(timestamp, struct.getNullableTimestamp("timestampValue"))
    assertFailsWith<IllegalStateException> { struct.getNullableTimestamp("int64Value") }
  }
}

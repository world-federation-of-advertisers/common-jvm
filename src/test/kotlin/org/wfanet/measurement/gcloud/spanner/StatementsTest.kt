// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Field
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toJson

@RunWith(JUnit4::class)
class StatementsTest {
  @Test
  fun `statement builds Statement`() {
    val boolValue = true
    val longValue = 1234L
    val doubleValue = 12.34
    val stringValue = "stringValue"
    val timestamp = Timestamp.now()
    val field = Field.newBuilder().setName("field_name").build()
    val cardinality = Field.Cardinality.CARDINALITY_REPEATED
    val table = "DummyTable"

    val statement: Statement =
      statement(table) {
        bind("BoolColumn" to boolValue)
        bind("LongColumn" to longValue)
        bind("DoubleColumn" to doubleValue)
        bind("StringColumn" to stringValue)
        bind("TimestampColumn" to timestamp)
        bind("EnumColumn").to(cardinality)
        bind("ProtoBytesColumn").to(field)
        bindJson("ProtoJsonColumn" to field)
      }

    assertThat(statement.sql).isEqualTo(table)
    assertThat(statement.parameters)
      .containsExactly(
        "BoolColumn",
        Value.bool(boolValue),
        "LongColumn",
        Value.int64(longValue),
        "DoubleColumn",
        Value.float64(doubleValue),
        "StringColumn",
        Value.string(stringValue),
        "TimestampColumn",
        Value.timestamp(timestamp),
        "EnumColumn",
        Value.protoEnum(cardinality),
        "ProtoBytesColumn",
        Value.protoMessage(field),
        "ProtoJsonColumn",
        Value.string(field.toJson()),
      )
  }
}

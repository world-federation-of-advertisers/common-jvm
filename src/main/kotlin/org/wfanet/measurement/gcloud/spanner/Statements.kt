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

import com.google.cloud.ByteArray
import com.google.cloud.Date
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.protobuf.Message
import com.google.protobuf.ProtocolMessageEnum

/** Binds the value that should be bound to the specified column. */
@JvmName("bindBoolean")
fun Statement.Builder.bind(columnValuePair: Pair<String, Boolean>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindBooleanBoxed")
fun Statement.Builder.bind(columnValuePair: Pair<String, Boolean?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindLong")
fun Statement.Builder.bind(columnValuePair: Pair<String, Long>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindLongBoxed")
fun Statement.Builder.bind(columnValuePair: Pair<String, Long?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindDouble")
fun Statement.Builder.bind(columnValuePair: Pair<String, Double>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindDoubleBoxed")
fun Statement.Builder.bind(columnValuePair: Pair<String, Double?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindString")
fun Statement.Builder.bind(columnValuePair: Pair<String, String?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindTimestamp")
fun Statement.Builder.bind(columnValuePair: Pair<String, Timestamp?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindDate")
fun Statement.Builder.bind(columnValuePair: Pair<String, Date?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindBytes")
fun Statement.Builder.bind(columnValuePair: Pair<String, ByteArray?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).to(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindProtoEnum")
fun Statement.Builder.bind(columnValuePair: Pair<String, ProtocolMessageEnum>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).toProtoEnum(value)
}

/** Binds the value that should be bound to the specified column. */
@JvmName("bindProtoMessageBytes")
fun Statement.Builder.bind(columnValuePair: Pair<String, Message?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).toProtoBytes(value)
}

/** Binds the JSON value that should be bound to the specified string column. */
fun Statement.Builder.bindJson(columnValuePair: Pair<String, Message?>): Statement.Builder {
  val (columnName, value) = columnValuePair
  return bind(columnName).toProtoJson(value)
}

/** Builds a [Statement]. */
fun makeStatement(sql: String, bind: Statement.Builder.() -> Unit): Statement =
  Statement.newBuilder(sql).apply(bind).build()

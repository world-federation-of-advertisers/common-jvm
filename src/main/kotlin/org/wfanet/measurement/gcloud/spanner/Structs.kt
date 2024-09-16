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
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.StructReader
import com.google.cloud.spanner.Type
import com.google.cloud.spanner.ValueBinder
import com.google.protobuf.AbstractMessage
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.ProtocolMessageEnum
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

/** Sets the value that should be bound to the specified column. */
@JvmName("setBoolean")
fun Struct.Builder.set(columnValuePair: Pair<String, Boolean>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setBooleanBoxed")
fun Struct.Builder.set(columnValuePair: Pair<String, Boolean?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setLong")
fun Struct.Builder.set(columnValuePair: Pair<String, Long>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setLongBoxed")
fun Struct.Builder.set(columnValuePair: Pair<String, Long?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDouble")
fun Struct.Builder.set(columnValuePair: Pair<String, Double>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDoubleBoxed")
fun Struct.Builder.set(columnValuePair: Pair<String, Double?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setString")
fun Struct.Builder.set(columnValuePair: Pair<String, String?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setTimestamp")
fun Struct.Builder.set(columnValuePair: Pair<String, Timestamp?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDate")
fun Struct.Builder.set(columnValuePair: Pair<String, Date?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setBytes")
fun Struct.Builder.set(columnValuePair: Pair<String, ByteArray?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setProtoEnum")
fun Struct.Builder.set(columnValuePair: Pair<String, ProtocolMessageEnum>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@Deprecated(message = "Use ValueBinder directly")
@JvmName("setProtoMessageBytes")
inline fun <reified T : AbstractMessage> Struct.Builder.set(
  columnValuePair: Pair<String, T?>
): Struct.Builder {
  val (columnName, value) = columnValuePair
  val binder: ValueBinder<Struct.Builder> = set(columnName)
  return if (value == null) {
    binder.to(null, ProtoReflection.getDescriptorForType(T::class))
  } else {
    binder.to(value)
  }
}

/** Sets the JSON value that should be bound to the specified string column. */
fun Struct.Builder.setJson(columnValuePair: Pair<String, Message?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoJson(value)
}

/**
 * Returns an [InternalId] with the value of a non-`NULL` column with type [Type.int64()]
 * [com.google.cloud.spanner.Type.int64].
 */
fun StructReader.getInternalId(columnName: String) = InternalId(getLong(columnName))

/**
 * Returns an [ExternalId] with the value of a non-`NULL` column with type [Type.int64()]
 * [com.google.cloud.spanner.Type.int64].
 */
fun StructReader.getExternalId(columnName: String) = ExternalId(getLong(columnName))

private fun <T> StructReader.nullOrValue(
  column: String,
  typeCode: Type.Code,
  getter: StructReader.(String) -> T,
): T? {
  val columnType = getColumnType(column).code
  check(columnType == typeCode) { "Cannot read $typeCode from $column, it has type $columnType" }
  return if (isNull(column)) null else getter(column)
}

/** Returns the value of a String column even if it is null. */
fun StructReader.getNullableString(column: String): String? =
  nullOrValue(column, Type.Code.STRING, StructReader::getString)

/** Returns the value of an Array of Structs column even if it is null. */
fun StructReader.getNullableStructList(column: String): MutableList<Struct>? =
  nullOrValue(column, Type.Code.ARRAY, StructReader::getStructList)

/** Returns the value of a Timestamp column even if it is null. */
fun StructReader.getNullableTimestamp(column: String): Timestamp? =
  nullOrValue(column, Type.Code.TIMESTAMP, StructReader::getTimestamp)

/** Returns the value of a INT64 column even if it is null. */
fun StructReader.getNullableLong(column: String): Long? =
  nullOrValue(column, Type.Code.INT64, StructReader::getLong)

/** Returns a bytes column as a Kotlin native ByteArray. This is useful for deserializing protos. */
fun StructReader.getBytesAsByteArray(column: String): kotlin.ByteArray =
  getBytes(column).toByteArray()

/** Returns a bytes column as a protobuf ByteString. */
fun StructReader.getBytesAsByteString(column: String): ByteString =
  ByteString.copyFrom(getBytes(column).asReadOnlyByteBuffer())

/** Parses a protobuf [Message] from a BYTES column. */
@Suppress("DeprecatedCallableAddReplaceWith") // Should use manual replacement to avoid reflection.
@Deprecated(message = "Use `getProtoMessage` overload which takes in a default message instance")
inline fun <reified T : AbstractMessage> StructReader.getProtoMessage(
  column: String,
  parser: Parser<T>,
): T = getProtoMessage(column, ProtoReflection.getDefaultInstance(T::class))

/** Builds a [Struct]. */
inline fun struct(bind: Struct.Builder.() -> Unit): Struct = Struct.newBuilder().apply(bind).build()

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

import com.google.cloud.ByteArray as CloudByteArray
import com.google.cloud.Date as CloudDate
import com.google.cloud.Timestamp as CloudTimestamp
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.StructReader
import com.google.cloud.spanner.Value
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.Timestamp
import java.math.BigInteger
import java.util.Date
import kotlin.reflect.KType
import kotlin.reflect.full.isSupertypeOf
import kotlin.reflect.typeOf
import org.wfanet.measurement.common.Reflect
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.parseFrom
import org.wfanet.measurement.gcloud.common.toProtoDate

/** Returns the value of a String column even if it is null. */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getNullableString(column: String): String? = get(column)

/** Returns the value of an Array of Structs column even if it is null. */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getNullableStructList(column: String): MutableList<Struct>? = get(column)

/** Returns the value of a Timestamp column even if it is null. */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getNullableTimestamp(column: String): CloudTimestamp? = get(column)

/** Returns the value of a INT64 column even if it is null. */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getNullableLong(column: String): Long? = get(column)

/** Returns a bytes column as a Kotlin native ByteArray. This is useful for deserializing protos. */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getBytesAsByteArray(column: String): ByteArray = get(column)

/** Returns a bytes column as a protobuf ByteString. */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getBytesAsByteString(column: String): ByteString = get(column)

/**
 * Returns an [InternalId] with the value of a non-`NULL` column with type [Type.int64()]
 * [com.google.cloud.spanner.Type.int64].
 */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getInternalId(columnName: String): InternalId = get(columnName)

/**
 * Returns an [ExternalId] with the value of a non-`NULL` column with type [Type.int64()]
 * [com.google.cloud.spanner.Type.int64].
 */
@Deprecated("Use `get` extension", ReplaceWith("this.get(column)"))
fun StructReader.getExternalId(columnName: String): ExternalId = get(columnName)

/** Parses a protobuf [Message] from a bytes column. */
fun <T : Message> StructReader.getProtoMessage(column: String, parser: Parser<T>): T =
  parser.parseFrom(getBytes(column))

/** Parses an enum from an INT64 Spanner column. */
fun <T : Enum<T>> StructReader.getProtoEnum(column: String, parser: (Int) -> T): T =
  parser(getLong(column).toInt())

/**
 * Returns the value for a column named [columnName].
 *
 * @throws IllegalArgumentException if [columnName] isn't found or [T] does not match the column's
 * type
 */
@OptIn(ExperimentalStdlibApi::class) // For `typeOf`.
inline operator fun <reified T> StructReader.get(columnName: String): T {
  val kType = typeOf<T>()

  val value: Value = getValue(columnName)
  if (value.isNull) {
    require(Reflect.isNullable<T>()) { "$kType is not nullable" }
    return null as T
  }

  @Suppress("UNCHECKED_CAST") return value.get(kType) as T
}

@PublishedApi
@OptIn(ExperimentalStdlibApi::class) // For `typeOf`.
internal fun Value.get(type: KType): Any? {
  return when {
    /** Built-in types */
    type.isSupertypeOf(typeOf<Boolean>()) -> bool
    type.isSupertypeOf(typeOf<Long>()) -> int64
    type.isSupertypeOf(typeOf<BigInteger>()) -> numeric
    type.isSupertypeOf(typeOf<Double>()) -> float64
    type.isSupertypeOf(typeOf<String>()) -> string
    type.isSupertypeOf(typeOf<CloudByteArray>()) -> bytes
    type.isSupertypeOf(typeOf<CloudTimestamp>()) -> timestamp
    type.isSupertypeOf(typeOf<CloudDate>()) -> date
    type.isSupertypeOf(typeOf<Struct>()) -> struct

    /** Arrays */
    type.isSupertypeOf(typeOf<MutableList<Boolean>>()) -> boolArray
    type.isSupertypeOf(typeOf<MutableList<Long>>()) -> int64Array
    type.isSupertypeOf(typeOf<MutableList<Double>>()) -> float64Array
    type.isSupertypeOf(typeOf<MutableList<BigInteger>>()) -> numericArray
    type.isSupertypeOf(typeOf<MutableList<String>>()) -> stringArray
    type.isSupertypeOf(typeOf<MutableList<CloudByteArray>>()) -> bytesArray
    type.isSupertypeOf(typeOf<MutableList<CloudTimestamp>>()) -> timestampArray
    type.isSupertypeOf(typeOf<MutableList<CloudDate>>()) -> dateArray
    type.isSupertypeOf(typeOf<MutableList<Struct>>()) -> structArray

    /** Conversions */
    type.isSupertypeOf(typeOf<ByteArray>()) -> bytes.toByteArray()
    type.isSupertypeOf(typeOf<ByteString>()) -> ByteString.copyFrom(bytes.asReadOnlyByteBuffer())
    type.isSupertypeOf(typeOf<Timestamp>()) -> timestamp.toProto()
    type.isSupertypeOf(typeOf<Date>()) -> CloudDate.toJavaUtilDate(date)
    type.isSupertypeOf(typeOf<com.google.type.Date>()) -> date.toProtoDate()
    type.isSupertypeOf(typeOf<InternalId>()) -> InternalId(int64)
    type.isSupertypeOf(typeOf<ExternalId>()) -> ExternalId(int64)

    /* Unsupported */
    else -> throw UnsupportedOperationException("Unsupported type $type")
  }
}

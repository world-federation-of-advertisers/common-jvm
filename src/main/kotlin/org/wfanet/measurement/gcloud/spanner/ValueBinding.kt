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

import com.google.cloud.spanner.ValueBinder
import com.google.protobuf.AbstractMessage
import com.google.protobuf.Message
import com.google.protobuf.ProtocolMessageEnum
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson

/** Binds to an [InternalId] value. */
fun <T> ValueBinder<T>.to(value: InternalId): T = to(value.value)

/** Binds to an [InternalId] value. */
@JvmName("toNullable") fun <T> ValueBinder<T>.to(value: InternalId?): T = to(value?.value)

/** Binds to an [ExternalId] value. */
fun <T> ValueBinder<T>.to(value: ExternalId): T = to(value.value)

/** Binds to an [ExternalId] value. */
@JvmName("toNullable") fun <T> ValueBinder<T>.to(value: ExternalId?): T = to(value?.value)

/** Bind to a protobuf message value. */
@Deprecated(
  message = "Use `to` overload that takes an `AbstractMessage`",
  replaceWith = ReplaceWith("to(value)"),
)
fun <T> ValueBinder<T>.toProtoBytes(value: AbstractMessage): T = to(value)

/** Bind to a protobuf message value. */
@Suppress("DeprecatedCallableAddReplaceWith") // Should use manual replacement to avoid reflection.
@Deprecated(
  message =
    "Use `to` overload that takes an `AbstractMessage` and a `Descriptors.Descriptor` for " +
      "nullable values"
)
@JvmName("toProtoBytesNullable")
inline fun <T, reified P : AbstractMessage> ValueBinder<T>.toProtoBytes(
  value: AbstractMessage?
): T {
  return if (value == null) {
    to(null, ProtoReflection.getDescriptorForType(P::class))
  } else {
    to(value)
  }
}

/** Bind a protobuf [Message] as a JSON string representation. */
fun <T> ValueBinder<T>.toProtoJson(value: Message?): T {
  return if (value == null) {
    to(null as String?)
  } else {
    to(value.toJson())
  }
}

/** Bind a protobuf enum value as an INT64 value. */
@Deprecated(message = "Use `toInt64`", replaceWith = ReplaceWith("toInt64(value)"))
fun <T> ValueBinder<T>.toProtoEnum(value: ProtocolMessageEnum): T = toInt64(value)

/** Bind a protobuf enum value as an INT64 value. */
fun <T> ValueBinder<T>.toInt64(value: ProtocolMessageEnum): T = to(value.numberAsLong)

/** Binds a collection of protobuf enum values as an ARRAY<INT64> value. */
@Deprecated(message = "Use `toInt64Array`", replaceWith = ReplaceWith("toInt64Array(values)"))
fun <T> ValueBinder<T>.toProtoEnumArray(values: Iterable<ProtocolMessageEnum>): T =
  toInt64Array(values)

/** Binds a collection of protobuf enum values as an ARRAY<INT64> value. */
fun <T> ValueBinder<T>.toInt64Array(values: Iterable<ProtocolMessageEnum>): T =
  toInt64Array(values.map(ProtocolMessageEnum::numberAsLong))

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
import com.google.protobuf.Message
import com.google.protobuf.ProtocolMessageEnum
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.common.toGcloudByteArray

/** Binds to an [InternalId] value. */
fun <T> ValueBinder<T>.to(value: InternalId): T = to(value.value)

/** Binds to an [InternalId] value. */
@JvmName("toNullable") fun <T> ValueBinder<T>.to(value: InternalId?): T = to(value?.value)

/** Binds to an [ExternalId] value. */
fun <T> ValueBinder<T>.to(value: ExternalId): T = to(value.value)

/** Binds to an [ExternalId] value. */
@JvmName("toNullable") fun <T> ValueBinder<T>.to(value: ExternalId?): T = to(value?.value)

/** Bind to a protobuf message as `BYTES`. */
fun <T> ValueBinder<T>.toProtoBytes(value: Message?): T = to(value?.toGcloudByteArray())

/** Binds to a protobuf message JSON value as a `STRING`. */
fun <T> ValueBinder<T>.toProtoJson(value: Message?): T = to(value?.toJson())

/** Binds to a protobuf enum value as an `INT64`. */
@Deprecated(message = "Use `toInt64`", replaceWith = ReplaceWith("toInt64(value)"))
fun <T> ValueBinder<T>.toProtoEnum(value: ProtocolMessageEnum): T = toInt64(value)

/** Binds to a protobuf enum value as an `INT64`. */
fun <T> ValueBinder<T>.toInt64(value: ProtocolMessageEnum): T = to(value.numberAsLong)

/** Binds to a collection of protobuf enum values as an `ARRAY<INT64>`. */
@Deprecated(message = "Use `toInt64Array`", replaceWith = ReplaceWith("toInt64Array(values)"))
fun <T> ValueBinder<T>.toProtoEnumArray(values: Iterable<ProtocolMessageEnum>): T =
  toInt64Array(values)

/** Binds to a collection of protobuf enum values as an `ARRAY<INT64>`. */
fun <T> ValueBinder<T>.toInt64Array(values: Iterable<ProtocolMessageEnum>): T =
  toInt64Array(values.map(ProtocolMessageEnum::numberAsLong))

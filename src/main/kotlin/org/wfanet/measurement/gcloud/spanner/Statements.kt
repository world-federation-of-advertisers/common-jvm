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
import com.google.protobuf.AbstractMessage
import com.google.protobuf.ProtocolMessageEnum
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

/** Binds the value that should be bound to the specified param. */
@JvmName("bindBoolean")
fun Statement.Builder.bind(paramValuePair: Pair<String, Boolean>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindBooleanBoxed")
fun Statement.Builder.bind(paramValuePair: Pair<String, Boolean?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindLong")
fun Statement.Builder.bind(paramValuePair: Pair<String, Long>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindLongBoxed")
fun Statement.Builder.bind(paramValuePair: Pair<String, Long?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindDouble")
fun Statement.Builder.bind(paramValuePair: Pair<String, Double>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindDoubleBoxed")
fun Statement.Builder.bind(paramValuePair: Pair<String, Double?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindString")
fun Statement.Builder.bind(paramValuePair: Pair<String, String?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindTimestamp")
fun Statement.Builder.bind(paramValuePair: Pair<String, Timestamp?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindDate")
fun Statement.Builder.bind(paramValuePair: Pair<String, Date?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindBytes")
fun Statement.Builder.bind(paramValuePair: Pair<String, ByteArray?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindInternalId")
fun Statement.Builder.bind(paramValuePair: Pair<String, InternalId>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@JvmName("bindExternalId")
fun Statement.Builder.bind(paramValuePair: Pair<String, ExternalId>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).to(value)
}

/** Binds the value that should be bound to the specified param. */
@Deprecated(message = "Use ValueBinder directly to avoid type ambiguity")
@JvmName("bindProtoEnum")
fun Statement.Builder.bind(paramValuePair: Pair<String, ProtocolMessageEnum>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).toInt64(value)
}

/** Binds the value that should be bound to the specified param. */
@Deprecated(message = "Use ValueBinder directly to avoid type ambiguity")
@JvmName("bindProtoMessageBytes")
inline fun <reified T : AbstractMessage> Statement.Builder.bind(
  paramValuePair: Pair<String, T?>
): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).toProtoBytes(value)
}

/** Binds the JSON value that should be bound to the specified string param. */
fun Statement.Builder.bindJson(paramValuePair: Pair<String, AbstractMessage?>): Statement.Builder {
  val (paramName, value) = paramValuePair
  return bind(paramName).toProtoJson(value)
}

/** Builds a [Statement]. */
fun statement(sql: String): Statement = Statement.newBuilder(sql).build()

/** Builds a [Statement]. */
inline fun statement(sql: String, bind: Statement.Builder.() -> Unit): Statement =
  Statement.newBuilder(sql).apply(bind).build()

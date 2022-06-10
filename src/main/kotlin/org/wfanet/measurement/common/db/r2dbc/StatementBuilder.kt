/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.db.r2dbc

import com.google.protobuf.Message
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

/** Builder for a SQL statement, which could be a query. */
@Builder
class StatementBuilder(private val baseSql: String) {
  private val namedBindings = mutableMapOf<String, Any>()
  private val namedNullBindings = mutableMapOf<String, Class<out Any>>()

  /** Adds a binding for the parameter named [name] to [value]. */
  fun bind(name: String, value: Any) {
    namedBindings[name] = value
  }
  fun bind(name: String, value: ExternalId) = bind(name, value.value)
  fun bind(name: String, value: InternalId) = bind(name, value.value)
  fun bind(name: String, value: Message) = bind(name, value.toByteString().asReadOnlyByteBuffer())

  /** Adds a binding for the parameter named [name] with type [type] to `NULL`. */
  fun <T : Any> bindNull(name: String, type: Class<T>) {
    namedNullBindings[name] = type
  }

  /** Adds a binding for the parameter named [name] with type [T] to `NULL`. */
  inline fun <reified T : Any> bindNull(name: String) = bindNull(name, T::class.java)

  internal fun build(connection: Connection): Statement {
    val statement = connection.createStatement(baseSql)
    for ((name, value) in namedBindings) {
      statement.bind(name, value)
    }
    for ((name, type) in namedNullBindings) {
      statement.bindNull(name, type)
    }
    return statement
  }

  companion object {
    fun statementBuilder(
      baseSql: String,
      bind: StatementBuilder.() -> Unit = {}
    ): StatementBuilder {
      return StatementBuilder(baseSql).apply(bind)
    }
  }
}

@DslMarker private annotation class Builder

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

/** An SQL statement with bound parameters. */
class BoundStatement
private constructor(
  private val baseSql: String,
  private val bindings: Map<String, Any>,
  private val nullBindings: Map<String, Class<out Any>>
) {
  @DslMarker private annotation class DslBuilder

  /** Builder for a SQL statement, which could be a query. */
  @DslBuilder
  abstract class Builder {
    /** Adds a binding for the parameter named [name] to [value]. */
    abstract fun bind(name: String, value: Any)
    /** Adds a binding for the parameter named [name] to [value]. */
    fun bind(name: String, value: ExternalId) = bind(name, value.value)
    /** Adds a binding for the parameter named [name] to [value]. */
    fun bind(name: String, value: InternalId) = bind(name, value.value)
    /** Adds a binding for the parameter named [name] to [value]. */
    fun bind(name: String, value: Message) = bind(name, value.toByteString().asReadOnlyByteBuffer())

    /** Adds a binding for the parameter named [name] with type [type] to `NULL`. */
    abstract fun <T : Any> bindNull(name: String, type: Class<T>)

    /** Adds a binding for the parameter named [name] with type [T] to `NULL`. */
    inline fun <reified T : Any> bindNull(name: String) = bindNull(name, T::class.java)
  }

  private class BuilderImpl(private val baseSql: String) : Builder() {
    private val bindings = mutableMapOf<String, Any>()
    private val nullBindings = mutableMapOf<String, Class<out Any>>()

    override fun bind(name: String, value: Any) {
      bindings[name] = value
    }

    override fun <T : Any> bindNull(name: String, type: Class<T>) {
      nullBindings[name] = type
    }

    /** Builds a [BoundStatement] from this builder. */
    fun build(): BoundStatement {
      return BoundStatement(baseSql, bindings, nullBindings)
    }
  }

  internal fun toStatement(connection: Connection): Statement {
    val statement = connection.createStatement(baseSql)
    for ((name, value) in bindings) {
      statement.bind(name, value)
    }
    for ((name, type) in nullBindings) {
      statement.bindNull(name, type)
    }
    return statement
  }

  companion object {
    internal fun boundStatement(baseSql: String, bind: Builder.() -> Unit): BoundStatement {
      return BuilderImpl(baseSql).apply(bind).build()
    }
  }
}

/** Builds a [BoundStatement]. */
fun boundStatement(baseSql: String, bind: BoundStatement.Builder.() -> Unit = {}): BoundStatement =
  BoundStatement.boundStatement(baseSql, bind)

/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.db.r2dbc.postgres

import com.google.protobuf.Message
import com.google.protobuf.ProtocolMessageEnum
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import kotlin.reflect.KClass
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

/** A PostgresSQL statement for statements with a values list and bound parameters. */
class ValuesListBoundStatement
private constructor(
  private val baseSql: String,
  private val bindings: Collection<Binding>,
): StatementBuilder {
  @DslMarker private annotation class DslBuilder

  /** Builder for a single statement binding. */
  @DslBuilder
  abstract class Binder {
    /** Binds the parameter named [name] to [value]. */
    fun bind(name: String, value: ExternalId?) = bind(name, value?.value)
    /** Binds the parameter named [name] to [value]. */
    fun bind(name: String, value: InternalId?) = bind(name, value?.value)
    /** Binds the parameter named [name] to [value]. */
    fun bind(name: String, value: Message?) =
      bind(name, value?.toByteString()?.asReadOnlyByteBuffer())
    /** Binds the parameter named [name] to [value]. */
    fun bind(name: String, value: ProtocolMessageEnum?) = bind(name, value?.number)

    /** Binds the parameter named [name] to [value]. */
    @JvmName("bindNullable")
    inline fun <reified T> bind(name: String, value: T) {
      if (value == null) {
        bindNull(name, T::class)
      } else {
        bind(name, value)
      }
    }

    /** Binds the parameter named [name] with type [kClass] to `NULL`. */
    @PublishedApi internal abstract fun bindNull(name: String, kClass: KClass<*>)

    /** Binds the parameter named [name] to [value]. */
    abstract fun <T : Any> bind(name: String, value: T)

    /** Binds the parameter [index], after adjusting for the number of values, to [value]. */
    fun bindValuesParam(index: Int, value: ExternalId?) = bindValuesParam(index, value?.value)
    /** Binds the parameter [index], after adjusting for the number of values, to [value]. */
    fun bindValuesParam(index: Int, value: InternalId?) = bindValuesParam(index, value?.value)
    /** Binds the parameter [index], after adjusting for the number of values, to [value]. */
    fun bindValuesParam(index: Int, value: Message?) =
      bindValuesParam(index, value?.toByteString()?.asReadOnlyByteBuffer())
    /** Binds the parameter [index], after adjusting for the number of values, to [value]. */
    fun bindValuesParam(index: Int, value: ProtocolMessageEnum?) = bindValuesParam(index, value?.number)

    /** Binds the parameter [index], after adjusting for the number of values, to [value]. */
    @JvmName("bindNullableValuesParam")
    inline fun <reified T> bindValuesParam(index: Int, value: T) {
      if (value == null) {
        bindNullValuesParam(index, T::class)
      } else {
        bindValuesParam(index, value)
      }
    }

    /** Binds the parameter [index], after adjusting for the number of values, to [value]. */
    abstract fun <T : Any> bindValuesParam(index: Int, value: T)

    /** Binds the parameter [index], after adjusting for the number of values, with type [kClass] to `NULL`. */
    @PublishedApi internal abstract fun bindNullValuesParam(index: Int, kClass: KClass<*>)
  }

  /** Builder for a SQL statement, which could be a query. */
  abstract class Builder : Binder() {
    /**
     * Binds values to parameters in the values list.
     */
    abstract fun addValuesBinding(bind: Binder.() -> Unit)
  }

  private class BinderImpl : Binder() {
    private val stringIndexValues = mutableMapOf<String, Any>()
    private val intIndexValues = mutableMapOf<Int, Any>()
    private val stringIndexNulls = mutableMapOf<String, Class<out Any?>>()
    private val intIndexNulls = mutableMapOf<Int, Class<out Any?>>()

    override fun <T : Any> bind(name: String, value: T) {
      stringIndexValues[name] = value
    }

    override fun <T : Any> bindValuesParam(index: Int, value: T) {
      intIndexValues[index] = value
    }

    override fun bindNull(name: String, kClass: KClass<*>) {
      stringIndexNulls[name] = kClass.javaObjectType
    }

    override fun bindNullValuesParam(index: Int, kClass: KClass<*>) {
      intIndexNulls[index] = kClass.javaObjectType
    }

    fun build() = Binding(stringIndexValues, intIndexValues, stringIndexNulls, intIndexNulls)
  }

  private class BuilderImpl(private val valuesStartIndex: Int, private val paramCount: Int, private val baseSql: String) : Builder() {
    private val binders: MutableList<BinderImpl> = mutableListOf()
    private var numValues = 0
    private var valuesCurIndex = valuesStartIndex
    private val initialBinder: BinderImpl
      get() =
        synchronized(binders) {
          if (binders.isEmpty()) {
            binders += BinderImpl()
          }
          binders.first()
        }

    override fun addValuesBinding(bind: Binder.() -> Unit) {
      this.apply(bind)
      numValues += 1
      valuesCurIndex += paramCount
    }

    override fun <T : Any> bind(name: String, value: T) = initialBinder.bind(name, value)

    override fun bindNull(name: String, kClass: KClass<*>) = initialBinder.bindNull(name, kClass)

    override fun <T : Any> bindValuesParam(index: Int, value: T) =
      initialBinder.bindValuesParam(index + valuesCurIndex, value)

    override fun bindNullValuesParam(index: Int, kClass: KClass<*>) =
      initialBinder.bindNullValuesParam(index + valuesCurIndex, kClass)

    /** Builds a [ValuesListBoundStatement] from this builder. */
    fun build(): ValuesListBoundStatement {
      val range = valuesStartIndex + 1 .. valuesCurIndex
      val params = range.toList()
      val chunkedParams = params.chunked(paramCount)
      val valuesList =
        chunkedParams.joinToString(separator = ",") { values ->
          values.map { value ->
            "$$value"
          }
            .joinToString(prefix = "(", postfix = ")")
        }
      return ValuesListBoundStatement(baseSql = baseSql.replace(VALUES_LIST_PLACEHOLDER, valuesList), binders.map { it.build() })
    }
  }

  override fun toStatement(connection: Connection): Statement {
    val statement = connection.createStatement(baseSql)
    if (bindings.isEmpty()) {
      return statement
    }

    val finalBinding =
      bindings.reduce { current: Binding, next: Binding ->
        statement.apply(current)
        statement.add()
        next
      }
    statement.apply(finalBinding)
    return statement
  }

  companion object {
    const val VALUES_LIST_PLACEHOLDER = "VALUES_LIST"
    internal fun boundStatement(valuesStartIndex: Int, paramCount: Int, baseSql: String, bind: Builder.() -> Unit): ValuesListBoundStatement {
      return BuilderImpl(valuesStartIndex = valuesStartIndex, paramCount = paramCount, baseSql = baseSql).apply(bind).build()
    }

    private fun Statement.apply(binding: Binding) {
      for ((name, value) in binding.stringIndexValues) {
        bind(name, value)
      }
      for ((index, value) in binding.intIndexValues) {
        bind(index, value)
      }
      for ((name, type) in binding.stringIndexNulls) {
        bindNull(name, type)
      }
      for ((index, type) in binding.intIndexNulls) {
        bindNull(index, type)
      }
    }
  }
}

private data class Binding(val stringIndexValues: Map<String, Any>, val intIndexValues: Map<Int, Any>, val stringIndexNulls: Map<String, Class<out Any?>>, val intIndexNulls: Map<Int, Class<out Any?>>)

/** Builds a [ValuesListBoundStatement]. */
fun valuesListBoundStatement(valuesStartIndex: Int, paramCount: Int, baseSql: String, bind: ValuesListBoundStatement.Builder.() -> Unit = {}): ValuesListBoundStatement =
  ValuesListBoundStatement.boundStatement(valuesStartIndex = valuesStartIndex, paramCount = paramCount, baseSql = baseSql, bind)

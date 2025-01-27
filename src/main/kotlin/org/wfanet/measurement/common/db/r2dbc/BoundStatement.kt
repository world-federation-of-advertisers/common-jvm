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
import com.google.protobuf.ProtocolMessageEnum
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import kotlin.reflect.KClass
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

/** An SQL statement with bound parameters. */
class BoundStatement
private constructor(private val baseSql: String, private val bindings: Collection<Binding>) {
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

    /** Binds the parameter named [name] to [value]. */
    abstract fun <T : Any> bind(name: String, value: T)

    /** Binds the parameter named [name] with type [kClass] to `NULL`. */
    @PublishedApi internal abstract fun bindNull(name: String, kClass: KClass<*>)

    /** Binds the parameter [index] to [value]. */
    fun bind(index: Int, value: ExternalId?) = bind(index, value?.value)

    /** Binds the parameter [index] to [value]. */
    fun bind(index: Int, value: InternalId?) = bind(index, value?.value)

    /** Binds the parameter [index] to [value]. */
    fun bind(index: Int, value: Message?) =
      bind(index, value?.toByteString()?.asReadOnlyByteBuffer())

    /** Binds the parameter [index] to [value]. */
    fun bind(index: Int, value: ProtocolMessageEnum?) = bind(index, value?.number)

    /** Binds the parameter [index] to [value]. */
    @JvmName("bindNullableIndex")
    inline fun <reified T> bind(index: Int, value: T) {
      if (value == null) {
        bindNull(index, T::class)
      } else {
        bind(index, value)
      }
    }

    /** Binds the parameter [index] to [value]. */
    abstract fun <T : Any> bind(index: Int, value: T)

    /** Binds the parameter [index] with type [kClass] to `NULL`. */
    @PublishedApi internal abstract fun bindNull(index: Int, kClass: KClass<*>)
  }

  /** Builder for a SQL statement, which could be a query. */
  abstract class Builder : Binder() {
    /**
     * Adds an additional binding.
     *
     * The following examples result in the same two bindings (Audi S4, Tesla Model 3):
     * ```kotlin
     * boundStatement("INSERT INTO Cars VALUES ($1, $2)") {
     *   bind("$1", "Audi")
     *   bind("$2", "S4")
     *   addBinding {
     *     bind("$1", "Tesla")
     *     bind("$2", "Model 3")
     *   }
     * }
     * ```
     * ```kotlin
     * boundStatement("INSERT INTO Cars VALUES ($1, $2)") {
     *   addBinding {
     *     bind("$1", "Audi")
     *     bind("$2", "S4")
     *   }
     *   addBinding {
     *     bind("$1", "Tesla")
     *     bind("$2", "Model 3")
     *   }
     * }
     * ```
     */
    abstract fun addBinding(bind: Binder.() -> Unit)

    /** Creates a [BoundStatement] from this [Builder]. */
    abstract fun build(baseSql: String): BoundStatement
  }

  private class BinderImpl : Binder() {
    private val stringIndexValues = mutableMapOf<String, Any>()
    private val intIndexValues = mutableMapOf<Int, Any>()
    private val stringIndexNulls = mutableMapOf<String, Class<out Any?>>()
    private val intIndexNulls = mutableMapOf<Int, Class<out Any?>>()

    override fun <T : Any> bind(name: String, value: T) {
      stringIndexValues[name] = value
    }

    override fun <T : Any> bind(index: Int, value: T) {
      intIndexValues[index] = value
    }

    override fun bindNull(name: String, kClass: KClass<*>) {
      stringIndexNulls[name] = kClass.javaObjectType
    }

    override fun bindNull(index: Int, kClass: KClass<*>) {
      intIndexNulls[index] = kClass.javaObjectType
    }

    fun build() = Binding(stringIndexValues, intIndexValues, stringIndexNulls, intIndexNulls)
  }

  private class BuilderImpl : Builder() {
    private val binders: MutableList<BinderImpl> = mutableListOf()
    private var bindable = true
    private val initialBinder: BinderImpl
      get() =
        synchronized(binders) {
          check(bindable) { "Cannot bind after adding a binding" }
          if (binders.isEmpty()) {
            binders += BinderImpl()
          }
          binders.first()
        }

    override fun addBinding(bind: Binder.() -> Unit) =
      synchronized(binders) {
        binders += BinderImpl().apply(bind)
        bindable = false
      }

    override fun <T : Any> bind(name: String, value: T) = initialBinder.bind(name, value)

    override fun bindNull(name: String, kClass: KClass<*>) = initialBinder.bindNull(name, kClass)

    override fun <T : Any> bind(index: Int, value: T) = initialBinder.bind(index, value)

    override fun bindNull(index: Int, kClass: KClass<*>) = initialBinder.bindNull(index, kClass)

    /** Builds a [BoundStatement] from this builder. */
    override fun build(baseSql: String): BoundStatement {
      return BoundStatement(baseSql, binders.map { it.build() })
    }
  }

  internal fun toStatement(connection: Connection): Statement {
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
    internal fun boundStatement(baseSql: String, bind: Builder.() -> Unit): BoundStatement {
      return BuilderImpl().apply(bind).build(baseSql)
    }

    internal fun builder(bind: Builder.() -> Unit): Builder {
      return BuilderImpl().apply(bind)
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

private data class Binding(
  val stringIndexValues: Map<String, Any>,
  val intIndexValues: Map<Int, Any>,
  val stringIndexNulls: Map<String, Class<out Any?>>,
  val intIndexNulls: Map<Int, Class<out Any?>>,
)

/** Builds a [BoundStatement]. */
fun boundStatement(baseSql: String, bind: BoundStatement.Builder.() -> Unit = {}): BoundStatement =
  BoundStatement.boundStatement(baseSql, bind)

/** Creates a [BoundStatement.Builder]. */
fun builder(bind: BoundStatement.Builder.() -> Unit = {}): BoundStatement.Builder =
  BoundStatement.builder(bind)

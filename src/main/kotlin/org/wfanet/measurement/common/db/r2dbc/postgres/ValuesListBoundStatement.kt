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
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

/** Contains a Postgres specific builder for [BoundStatement]s with a values list. */
class ValuesListBoundStatement private constructor() {
  @DslMarker private annotation class DslBuilder

  @DslBuilder
  class ValuesListBoundStatementBuilder(
    valuesStartIndex: Int,
    @PublishedApi internal val paramCount: Int,
    @PublishedApi internal val binder: BoundStatement.Binder,
  ) {
    @PublishedApi
    internal var valuesCurIndex = valuesStartIndex
      private set

    fun addValuesBinding(bind: ValuesListBoundStatementBuilder.() -> Unit) {
      this.apply(bind)
      valuesCurIndex += paramCount
    }

    fun bindValuesParam(index: Int, value: ExternalId?) =
      binder.bind(index + valuesCurIndex, value?.value)

    fun bindValuesParam(index: Int, value: InternalId?) =
      binder.bind(index + valuesCurIndex, value?.value)

    fun bindValuesParam(index: Int, value: Message?) =
      binder.bind(index + valuesCurIndex, value?.toByteString()?.asReadOnlyByteBuffer())

    fun bindValuesParam(index: Int, value: ProtocolMessageEnum?) =
      binder.bind(index + valuesCurIndex, value?.number)

    inline fun <reified T : Any> bindValuesParam(index: Int, value: T?) =
      binder.bind(index + valuesCurIndex, value)

    fun bind(name: String, value: ExternalId?) = binder.bind(name, value?.value)

    fun bind(name: String, value: InternalId?) = binder.bind(name, value?.value)

    fun bind(name: String, value: Message?) =
      binder.bind(name, value?.toByteString()?.asReadOnlyByteBuffer())

    fun bind(name: String, value: ProtocolMessageEnum?) = binder.bind(name, value?.number)

    inline fun <reified T : Any> bind(name: String, value: T?) {
      binder.bind(name, value)
    }

    fun bind(index: Int, value: ExternalId?) = binder.bind(index, value?.value)

    fun bind(index: Int, value: InternalId?) = binder.bind(index, value?.value)

    fun bind(index: Int, value: Message?) =
      binder.bind(index, value?.toByteString()?.asReadOnlyByteBuffer())

    fun bind(index: Int, value: ProtocolMessageEnum?) = binder.bind(index, value?.number)

    inline fun <reified T : Any> bind(index: Int, value: T?) {
      binder.bind(index, value)
    }
  }

  companion object {
    const val VALUES_LIST_PLACEHOLDER = "VALUES_LIST"

    internal fun valuesListBoundStatement(
      valuesStartIndex: Int,
      paramCount: Int,
      numValues: Int,
      baseSql: String,
      bind: ValuesListBoundStatementBuilder.() -> Unit = {},
    ): BoundStatement {
      val range = valuesStartIndex + 1..numValues * paramCount + valuesStartIndex
      val params = range.toList()
      val chunkedParams = params.chunked(paramCount)
      val valuesList =
        chunkedParams.joinToString(separator = ",") { values ->
          values.joinToString(prefix = "(", postfix = ")") { value -> "$$value" }
        }

      return boundStatement(baseSql.replace(VALUES_LIST_PLACEHOLDER, valuesList)) {
        ValuesListBoundStatementBuilder(valuesStartIndex, paramCount, this).apply(bind)
      }
    }
  }
}

/** Builds a [BoundStatement] with a values list. */
fun valuesListBoundStatement(
  valuesStartIndex: Int,
  paramCount: Int,
  numValues: Int,
  baseSql: String,
  bind: ValuesListBoundStatement.ValuesListBoundStatementBuilder.() -> Unit = {},
): BoundStatement =
  ValuesListBoundStatement.valuesListBoundStatement(
    valuesStartIndex = valuesStartIndex,
    paramCount = paramCount,
    numValues = numValues,
    baseSql = baseSql,
    bind,
  )

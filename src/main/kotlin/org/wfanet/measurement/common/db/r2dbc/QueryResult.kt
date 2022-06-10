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

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.kotlin.toByteString
import io.r2dbc.spi.Readable
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import java.nio.ByteBuffer
import java.util.function.Function
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.reactive.asFlow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

open class QueryResult internal constructor(private val result: Result) {
  /**
   * Consumes the result [ResultRow]s using the specified [transform].
   *
   * Note that the [ResultRow] objects are only valid inside the context of [transform].
   */
  open fun <T : Any> consume(transform: Function<in ResultRow, out T>): Flow<T> {
    return result.map { row, _ -> transform.apply(ResultRow(row)) }.asFlow()
  }
}

internal class SingleUseQueryResult(
  result: Result,
  private val closeTransaction: suspend () -> Unit
) : QueryResult(result) {

  override fun <T : Any> consume(transform: Function<in ResultRow, out T>): Flow<T> {
    return super.consume(transform).onCompletion { closeTransaction() }
  }
}

class ResultRow(private val delegate: Row) {
  val metadata: RowMetadata
    get() = delegate.metadata

  fun asReadable(): Readable = delegate

  inline operator fun <reified T : Any> get(name: String): T? = asReadable().get<T>(name)

  inline fun <reified T : Any> getValue(name: String): T = asReadable().getValue(name)

  fun <T : Message> getProtoMessage(name: String, parser: Parser<T>): T =
    delegate.getProtoMessage(name, parser)

  fun <T : Message> getProtoMessageOrNull(name: String, parser: Parser<T>): T? =
    delegate.getProtoMessageOrNull(name, parser)
}

@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline operator fun <reified T : Any> Readable.get(name: String): T? = get(name, T::class)

inline fun <reified T : Any> Readable.getValue(name: String): T = checkNotNull(get<T>(name))

fun <T : Any> Readable.get(name: String, kClass: KClass<T>): T? {
  require(!kClass.isSubclassOf(Message::class)) { "Use getProtoMessage" }

  // Handle custom type conversions.
  @Suppress("UNCHECKED_CAST")
  return when (kClass) {
    ByteString::class -> get(name, ByteBuffer::class)?.toByteString() as T?
    InternalId::class -> get(name, Long::class)?.let { InternalId(it) } as T?
    ExternalId::class -> get(name, Long::class)?.let { ExternalId(it) } as T?

    // Note that this is not `kClass.java`. The underlying `get` may support Java builtin types but
    // not their Kotlin counterparts, e.g. `java.lang.Integer` instead of `kotlin.Int`.
    else -> get(name, kClass.javaObjectType)
  }
}

fun <T : Message> Readable.getProtoMessage(name: String, parser: Parser<T>): T =
  parser.parseFrom(get<ByteBuffer>(name))

fun <T : Message> Readable.getProtoMessageOrNull(name: String, parser: Parser<T>): T? =
  get<ByteBuffer>(name)?.let { parser.parseFrom(it) }

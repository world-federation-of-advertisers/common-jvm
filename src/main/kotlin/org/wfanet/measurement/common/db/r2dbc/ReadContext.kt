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

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.TransactionDefinition
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle

/**
 * A transaction context for reading.
 *
 * [close] must be called when done with this context.
 */
interface ReadContext {
  /**
   * Executes a query.
   *
   * @param query a query which produces a single [Result]
   * @return the resulting [Row]s
   */
  suspend fun executeQuery(query: BoundStatement): QueryResult

  /** Closes the underlying [Connection]. */
  suspend fun close()

  /**
   * Rolls back the transaction state.
   *
   * This transaction context may be reused.
   */
  suspend fun rollback()
}

internal open class ReadContextImpl
protected constructor(
  protected val connection: Connection,
  private val transactionDefinition: TransactionDefinition,
) : ReadContext {

  override suspend fun executeQuery(query: BoundStatement): QueryResult {
    val result: Result = executeInTransaction {
      query.toStatement(connection).execute().awaitSingle()
    }
    return QueryResult(result)
  }

  override suspend fun close() {
    connection.close().awaitFirstOrNull()
  }

  override suspend fun rollback() {
    connection.rollbackTransaction().awaitFirstOrNull()
  }

  /**
   * Executes [block] within the transaction, beginning one if the connection is not already in a
   * transaction.
   */
  protected suspend fun <T> executeInTransaction(block: suspend () -> T): T {
    if (connection.isAutoCommit) {
      beginTransaction(connection, transactionDefinition)
    }
    return block()
  }

  companion object {
    fun create(connection: Connection, transactionDefinition: TransactionDefinition): ReadContext {
      return ReadContextImpl(connection, transactionDefinition)
    }

    suspend fun beginTransaction(connection: Connection, definition: TransactionDefinition) {
      try {
        connection.beginTransaction(definition).awaitFirstOrNull()
      } catch (e: Exception) {
        connection.close().awaitFirstOrNull()
        throw e
      }
    }
  }
}

internal class SingleUseReadContext
private constructor(connection: Connection, transactionDefinition: TransactionDefinition) :
  ReadContextImpl(connection, transactionDefinition) {

  override suspend fun executeQuery(query: BoundStatement): QueryResult {
    val result: Result =
      try {
        executeInTransaction { query.toStatement(connection).execute().awaitSingle() }
      } catch (e: Exception) {
        close()
        throw e
      }
    return SingleUseQueryResult(result, ::close)
  }

  companion object {
    fun create(connection: Connection, transactionDefinition: TransactionDefinition): ReadContext {
      return SingleUseReadContext(connection, transactionDefinition)
    }
  }
}

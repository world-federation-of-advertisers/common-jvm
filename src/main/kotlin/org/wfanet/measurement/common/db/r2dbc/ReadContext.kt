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
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle

/** A transaction context for reading. */
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
   * Rollbacks the transaction.
   *
   * Note: Using this on a new transaction causes the transaction to be stuck in the IDLE state.
   */
  suspend fun rollback()
}

internal open class ReadContextImpl protected constructor(protected val connection: Connection) :
  ReadContext {

  override suspend fun executeQuery(query: BoundStatement): QueryResult {
    val result: Result = query.toStatement(connection).execute().awaitSingle()
    return QueryResult(result)
  }

  override suspend fun close() {
    try {
      connection.close().awaitFirstOrNull()
    } catch (_: Exception) {}
  }

  override suspend fun rollback() {
    connection.rollbackTransaction().awaitFirstOrNull()
  }

  companion object {
    suspend fun create(
      connection: Connection,
      transactionDefinition: TransactionDefinition
    ): ReadContext {
      beginTransaction(connection, transactionDefinition)
      return ReadContextImpl(connection)
    }

    suspend fun beginTransaction(connection: Connection, definition: TransactionDefinition) {
      try {
        connection.beginTransaction(definition).awaitFirstOrNull()
      } catch (e: Exception) {
        try {
          connection.close().awaitFirstOrNull()
        } catch (_: Exception) {}
        throw e
      }
    }
  }
}

internal class SingleUseReadContext private constructor(connection: Connection) :
  ReadContextImpl(connection) {

  override suspend fun executeQuery(query: BoundStatement): QueryResult {
    val result: Result =
      try {
        query.toStatement(connection).execute().awaitSingle()
      } catch (e: Exception) {
        close()
        throw e
      }
    return SingleUseQueryResult(result, ::close)
  }

  companion object {
    suspend fun create(
      connection: Connection,
      transactionDefinition: TransactionDefinition
    ): ReadContext {
      beginTransaction(connection, transactionDefinition)
      return SingleUseReadContext(connection)
    }
  }
}

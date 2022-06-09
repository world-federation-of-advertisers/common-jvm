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
import io.r2dbc.spi.TransactionDefinition
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull

/** A transaction context for reading and writing. */
interface ReadWriteContext : ReadContext {
  /** Executes a DML statement. */
  suspend fun executeStatement(statement: StatementBuilder): StatementResult

  /**
   * Commits the transaction.
   *
   * This closes the underlying connection.
   */
  suspend fun commit()
}

internal class ReadWriteContextImpl private constructor(connection: Connection) :
  ReadWriteContext, ReadContextImpl(connection) {

  override suspend fun executeStatement(statement: StatementBuilder): StatementResult {
    val result: Result = statement.build(connection).execute().awaitFirst()
    return StatementResult(result.rowsUpdated.awaitFirst())
  }

  override suspend fun commit() {
    connection.commitTransaction().awaitFirstOrNull()
    close()
  }

  companion object {
    suspend fun create(
      connection: Connection,
      transactionDefinition: TransactionDefinition
    ): ReadWriteContext {
      beginTransaction(connection, transactionDefinition)
      return ReadWriteContextImpl(connection)
    }
  }
}

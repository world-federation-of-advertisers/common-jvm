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
import io.r2dbc.spi.TransactionDefinition

typealias ConnectionProvider = suspend () -> Connection

/** A client for a database using R2DBC. */
abstract class DatabaseClient(private val getConnection: ConnectionProvider) {
  protected abstract val readTransactionDefinition: TransactionDefinition
  protected abstract val readWriteTransactionDefinition: TransactionDefinition

  /**
   * Returns a new [ReadContext] for executing a single query.
   *
   * This will close the underlying connection when the query results have been consumed.
   */
  suspend fun singleUse(): ReadContext {
    return SingleUseReadContext.create(getConnection(), readTransactionDefinition)
  }

  /** Returns a new [ReadContext]. */
  suspend fun readTransaction(): ReadContext {
    return ReadContextImpl.create(getConnection(), readTransactionDefinition)
  }

  /** Returns a new [ReadWriteContext]. */
  suspend fun readWriteTransaction(): ReadWriteContext {
    return ReadWriteContextImpl.create(getConnection(), readWriteTransactionDefinition)
  }
}

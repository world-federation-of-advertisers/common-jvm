// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.AsyncResultSet
import com.google.cloud.spanner.AsyncRunner
import com.google.cloud.spanner.CommitResponse
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options.QueryOption
import com.google.cloud.spanner.Options.ReadOption
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.ReadOnlyTransaction
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.TransactionContext
import java.time.Duration
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.time.withTimeout
import org.wfanet.measurement.gcloud.common.asApiFuture
import org.wfanet.measurement.gcloud.common.await

typealias TransactionWork<R> =
  suspend CoroutineScope.(txn: AsyncDatabaseClient.TransactionContext) -> R

/**
 * Non-blocking wrapper around [dbClient] using asynchronous Spanner Java API.
 *
 * This class only exposes the methods used by this project and not the complete API.
 *
 * @param dbClient [DatabaseClient] to wrap
 * @param executor [Executor] for read-write transactions
 */
class AsyncDatabaseClient(private val dbClient: DatabaseClient, private val executor: Executor) {
  /** @see DatabaseClient.singleUse */
  fun singleUse(bound: TimestampBound = TimestampBound.strong()): ReadContext {
    return ReadContextImpl(dbClient.singleUse(bound))
  }

  /** @see DatabaseClient.singleUseReadOnlyTransaction */
  fun singleUseReadOnlyTransaction(
    bound: TimestampBound = TimestampBound.strong()
  ): ReadOnlyTransaction {
    return ReadOnlyTransactionImpl(dbClient.singleUseReadOnlyTransaction(bound))
  }

  /** @see DatabaseClient.readOnlyTransaction */
  fun readOnlyTransaction(bound: TimestampBound = TimestampBound.strong()): ReadOnlyTransaction {
    return ReadOnlyTransactionImpl(dbClient.readOnlyTransaction(bound))
  }

  /** @see DatabaseClient.readWriteTransaction */
  fun readWriteTransaction(): TransactionRunner {
    return TransactionRunnerImpl(dbClient.runAsync())
  }

  /** @see DatabaseClient.write */
  suspend fun write(mutations: Iterable<Mutation>) {
    readWriteTransaction().run { txn -> txn.buffer(mutations) }
  }

  /** @see DatabaseClient.write */
  suspend fun write(vararg mutations: Mutation) {
    write(mutations.asIterable())
  }

  /**
   * Suspends until the [AsyncDatabaseClient] is ready, throwing a
   * [kotlinx.coroutines.TimeoutCancellationException] on timeout.
   */
  suspend fun waitUntilReady(timeout: Duration) {
    // Issue a no-op query and attempt to get the result in order to verify that
    // the Spanner DB connection is ready.  In testing, it was observed that
    // attempting to do this would block forever if an emulator host was specified
    // with nothing listening there. Therefore, we use the async API.
    val results = singleUse().executeQuery(Statement.of("SELECT 1"))
    withTimeout(timeout) {
      checkNotNull(results.singleOrNull()) { "No results from Spanner ready query" }
    }
  }

  /** Async coroutine version of [com.google.cloud.spanner.ReadContext] */
  interface ReadContext : AutoCloseable {
    /** @see [com.google.cloud.spanner.ReadContext.read] */
    fun read(
      table: String,
      keys: KeySet,
      columns: Iterable<String>,
      vararg options: ReadOption,
    ): Flow<Struct>

    /** @see com.google.cloud.spanner.ReadContext.readRow */
    suspend fun readRow(table: String, key: Key, columns: Iterable<String>): Struct?

    /** @see com.google.cloud.spanner.ReadContext.readRowUsingIndex */
    suspend fun readRowUsingIndex(
      table: String,
      index: String,
      key: Key,
      columns: Iterable<String>,
    ): Struct?

    /** @see com.google.cloud.spanner.ReadContext.readRowUsingIndex */
    suspend fun readRowUsingIndex(
      table: String,
      index: String,
      key: Key,
      vararg columns: String,
    ): Struct?

    /** @see com.google.cloud.spanner.ReadContext.readUsingIndex */
    suspend fun readUsingIndex(
      table: String,
      index: String,
      keySet: KeySet,
      columns: Iterable<String>,
    ): Flow<Struct>

    /** @see com.google.cloud.spanner.ReadContext.executeQuery */
    fun executeQuery(statement: Statement, vararg options: QueryOption): Flow<Struct>
  }

  /** Async coroutine version of [com.google.cloud.spanner.ReadOnlyTransaction]. */
  interface ReadOnlyTransaction : ReadContext {
    /** @see com.google.cloud.spanner.ReadOnlyTransaction.getReadTimestamp */
    val readTimestamp: Timestamp
  }

  /** Async coroutine version of [com.google.cloud.spanner.TransactionRunner]. */
  interface TransactionRunner {
    /**
     * Executes a read/write transaction with retries as necessary.
     *
     * @param doWork function that does work inside a transaction
     *
     * This acts as a coroutine builder. [doWork] has a [CoroutineScope] receiver to ensure that
     * coroutine builders called from it run in the [CoroutineScope] defined by this function.
     *
     * @see com.google.cloud.spanner.TransactionRunner.run
     */
    suspend fun <R> run(doWork: TransactionWork<R>): R

    /** Alias for [run]. */
    @Deprecated(message = "Use run", replaceWith = ReplaceWith("run(doWork)"))
    suspend fun <R> execute(doWork: TransactionWork<R>): R = run(doWork)

    /** @see com.google.cloud.spanner.TransactionRunner.getCommitTimestamp */
    suspend fun getCommitTimestamp(): Timestamp

    /** @see com.google.cloud.spanner.TransactionRunner.getCommitTimestamp */
    suspend fun getCommitResponse(): CommitResponse
  }

  /** Async coroutine version of [com.google.cloud.spanner.TransactionContext]. */
  interface TransactionContext : ReadContext {
    /** @see com.google.cloud.spanner.TransactionContext.buffer */
    fun buffer(mutation: Mutation)

    /** @see com.google.cloud.spanner.TransactionContext.buffer */
    fun buffer(mutations: Iterable<Mutation>)

    /** @see com.google.cloud.spanner.TransactionContext.executeUpdate */
    suspend fun executeUpdate(statement: Statement): Long
  }

  private inner class TransactionRunnerImpl(private val delegate: AsyncRunner) : TransactionRunner {
    override suspend fun <R> run(doWork: TransactionWork<R>): R {
      try {
        return delegate.run(executor, doWork)
      } catch (e: SpannerException) {
        throw e.wrappedException ?: e
      }
    }

    override suspend fun getCommitTimestamp(): Timestamp = delegate.commitTimestamp.await()

    override suspend fun getCommitResponse(): CommitResponse = delegate.commitResponse.await()
  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}

private class ReadContextImpl(private val readContext: ReadContext) :
  AsyncDatabaseClient.ReadContext, AutoCloseable by readContext {

  override fun read(
    table: String,
    keys: KeySet,
    columns: Iterable<String>,
    vararg options: ReadOption,
  ): Flow<Struct> = flowFrom { readContext.readAsync(table, keys, columns, *options) }

  override suspend fun readRow(table: String, key: Key, columns: Iterable<String>): Struct? {
    return readContext.readRowAsync(table, key, columns).await()
  }

  override suspend fun readRowUsingIndex(
    table: String,
    index: String,
    key: Key,
    columns: Iterable<String>,
  ): Struct? {
    return readContext.readRowUsingIndexAsync(table, index, key, columns).await()
  }

  override suspend fun readRowUsingIndex(
    table: String,
    index: String,
    key: Key,
    vararg columns: String,
  ): Struct? {
    return readRowUsingIndex(table, index, key, columns.asIterable())
  }

  override suspend fun readUsingIndex(
    table: String,
    index: String,
    keySet: KeySet,
    columns: Iterable<String>,
  ): Flow<Struct> = flowFrom { readContext.readUsingIndexAsync(table, index, keySet, columns) }

  override fun executeQuery(statement: Statement, vararg options: QueryOption): Flow<Struct> =
    flowFrom {
      readContext.executeQueryAsync(statement, *options)
    }
}

private class ReadOnlyTransactionImpl(private val delegate: ReadOnlyTransaction) :
  AsyncDatabaseClient.ReadOnlyTransaction,
  AsyncDatabaseClient.ReadContext by ReadContextImpl(delegate) {
  override val readTimestamp: Timestamp
    get() = delegate.readTimestamp
}

private class TransactionContextImpl(private val txn: TransactionContext) :
  AsyncDatabaseClient.TransactionContext, AsyncDatabaseClient.ReadContext by ReadContextImpl(txn) {

  override fun buffer(mutation: Mutation) = txn.buffer(mutation)

  override fun buffer(mutations: Iterable<Mutation>) = txn.buffer(mutations)

  override suspend fun executeUpdate(statement: Statement): Long {
    return txn.executeUpdateAsync(statement).await()
  }
}

@OptIn(ExperimentalStdlibApi::class) // For CoroutineDispatcher
private fun flowFrom(executeQuery: () -> AsyncResultSet): Flow<Struct> = callbackFlow {
  // Defer executing the query until we're in the producer scope.
  val resultSet: AsyncResultSet = executeQuery()

  val dispatcher = coroutineContext[CoroutineDispatcher] ?: Dispatchers.Default
  resultSet.setCallback(dispatcher.asExecutor()) { cursor ->
    try {
      cursorReady(cursor)
    } catch (t: Throwable) {
      close(t)
      resultSet.cancel()
      AsyncResultSet.CallbackResponse.DONE
    }
  }
  awaitClose { resultSet.close() }
}

private fun ProducerScope<Struct>.cursorReady(
  cursor: AsyncResultSet
): AsyncResultSet.CallbackResponse {
  while (true) {
    when (checkNotNull(cursor.tryNext())) {
      AsyncResultSet.CursorState.OK -> {
        val currentRow = cursor.currentRowAsStruct
        if (trySend(currentRow).isSuccess) {
          continue
        }
        launch(CoroutineName("AsyncResultSet cursorReady resume")) {
          try {
            send(currentRow)
            cursor.resume()
          } catch (t: Throwable) {
            this@cursorReady.cancel(CancellationException("Error resuming AsyncResultSet", t))
            cursor.cancel()
          }
        }
        return AsyncResultSet.CallbackResponse.PAUSE
      }
      AsyncResultSet.CursorState.NOT_READY -> return AsyncResultSet.CallbackResponse.CONTINUE
      AsyncResultSet.CursorState.DONE -> {
        close()
        return AsyncResultSet.CallbackResponse.DONE
      }
    }
  }
}

private suspend fun <T> AsyncRunner.run(executor: Executor, doWork: TransactionWork<T>): T {
  // The Spanner client library may call the async callback multiple times as it performs retries on
  // certain exceptions. Therefore, we use supervisorScope to prevent these exceptions from
  // cancelling the parent job.
  return supervisorScope {
    val asyncWork =
      AsyncRunner.AsyncWork { txn ->
        async {
            // Create a separate coroutine scope so that we have normal (non-supervisor) structured
            // concurrency semantics for the actual work.
            coroutineScope { doWork(txn.asAsyncTransaction()) }
          }
          .asApiFuture()
      }

    // Wait inside the supervisor scope to ensure any callback futures are created before the scope
    // completes, and also propagate cancellation to child coroutines.
    runAsync(asyncWork, executor).await()
  }
}

private fun TransactionContext.asAsyncTransaction(): AsyncDatabaseClient.TransactionContext =
  TransactionContextImpl(this)

fun Spanner.getAsyncDatabaseClient(
  databaseId: DatabaseId,
  executor: Executor = Executors.newSingleThreadExecutor(),
) = AsyncDatabaseClient(getDatabaseClient(databaseId), executor)

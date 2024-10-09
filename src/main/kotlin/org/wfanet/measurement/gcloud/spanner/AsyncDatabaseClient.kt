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
import com.google.cloud.spanner.AbortedException
import com.google.cloud.spanner.AsyncResultSet
import com.google.cloud.spanner.AsyncTransactionManager
import com.google.cloud.spanner.CommitResponse
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Options.QueryOption
import com.google.cloud.spanner.Options.ReadOption
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.ReadOnlyTransaction
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.TransactionContext
import com.google.common.util.concurrent.MoreExecutors
import java.time.Duration
import java.util.concurrent.Executor
import java.util.logging.Logger
import kotlin.coroutines.ContinuationInterceptor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
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
 */
class AsyncDatabaseClient(private val dbClient: DatabaseClient) {
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
  fun readWriteTransaction(vararg options: Options.TransactionOption): TransactionRunner {
    return TransactionRunnerImpl(dbClient.transactionManagerAsync(*options))
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
  interface TransactionRunner : AutoCloseable {
    /**
     * Executes a read/write transaction with retries as necessary.
     *
     * @param doWork function that does work inside a transaction
     *
     * This acts as a coroutine builder. [doWork] has a [CoroutineScope] receiver to ensure that
     * coroutine builders called from it run in the [CoroutineScope] defined by this function.
     *
     * This will close the [TransactionRunner].
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

/**
 * [AsyncDatabaseClient.TransactionRunner] implementation.
 *
 * This wraps [AsyncTransactionManager] rather than [com.google.cloud.spanner.AsyncRunner] as the
 * latter makes blocking calls.
 *
 * TODO(googleapis/java-spanner#2698): Consider switching to AsyncRunner when fixed.
 */
private class TransactionRunnerImpl(private val manager: AsyncTransactionManager) :
  AsyncDatabaseClient.TransactionRunner {

  override suspend fun <R> run(doWork: TransactionWork<R>): R {
    return manager.use { manager ->
      manager.runTransaction(doWork).also { manager.closeAsync().await() }
    }
  }

  override suspend fun getCommitTimestamp(): Timestamp = getCommitResponse().commitTimestamp

  override suspend fun getCommitResponse(): CommitResponse = manager.commitResponse.await()

  override fun close() {
    manager.close()
  }

  private class TransactionContextImpl(private val delegate: TransactionContext) :
    AsyncDatabaseClient.TransactionContext,
    AsyncDatabaseClient.ReadContext by ReadContextImpl(delegate) {
    override fun buffer(mutation: Mutation) = delegate.buffer(mutation)

    override fun buffer(mutations: Iterable<Mutation>) = delegate.buffer(mutations)

    override suspend fun executeUpdate(statement: Statement): Long =
      delegate.executeUpdateAsync(statement).await()
  }

  companion object {
    private suspend fun <R> AsyncTransactionManager.runTransaction(doWork: TransactionWork<R>): R =
      supervisorScope {
        // This uses a supervisor scope so exceptions thrown by `doWork` do not cancel the coroutine
        // prematurely.
        var result: R? = null
        var transactionFuture: AsyncTransactionManager.TransactionContextFuture = beginAsync()
        while (true) {
          coroutineContext.ensureActive()
          try {
            val committableFuture: AsyncTransactionManager.AsyncTransactionStep<Void, R> =
              transactionFuture.then(
                { txn, _ ->
                  async {
                      // Create a separate coroutine scope so that we have normal (non-supervisor)
                      // structured concurrency semantics for the actual work.
                      coroutineScope { doWork(TransactionContextImpl(txn)) }
                    }
                    .asApiFuture()
                },
                MoreExecutors.directExecutor(),
              )

            // Await before committing so that any exception gets thrown here.
            result = committableFuture.await()
            committableFuture.commitAsync().await()
            break
          } catch (e: AbortedException) {
            delay(e.retryDelayInMillis)
            transactionFuture = resetForRetryAsync()
          }
        }
        result!!
      }
  }
}

private fun flowFrom(executeQuery: () -> AsyncResultSet): Flow<Struct> {
  return callbackFlow {
      // Defer executing the query until we're in the producer scope.
      val resultSet: AsyncResultSet = executeQuery()

      resultSet.setCallback(underlyingExecutor) { cursor ->
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
    .buffer(Channel.RENDEZVOUS)
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

private val CoroutineScope.underlyingExecutor: Executor
  get() {
    val dispatcher: ContinuationInterceptor? = coroutineContext[ContinuationInterceptor]
    // Dispatchers.Unconfined throws when used as an executor.
    if (dispatcher is CoroutineDispatcher && dispatcher !== Dispatchers.Unconfined) {
      return dispatcher.asExecutor()
    }
    return Dispatchers.Default.asExecutor()
  }

fun Spanner.getAsyncDatabaseClient(databaseId: DatabaseId) =
  AsyncDatabaseClient(getDatabaseClient(databaseId))

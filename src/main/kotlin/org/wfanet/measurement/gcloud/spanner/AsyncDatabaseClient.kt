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

import com.google.api.core.ApiFuture
import com.google.cloud.Timestamp
import com.google.cloud.spanner.AsyncResultSet
import com.google.cloud.spanner.AsyncRunner
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options.QueryOption
import com.google.cloud.spanner.Options.ReadOption
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.TransactionContext
import java.time.Duration
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.gcloud.common.DirectExecutor
import org.wfanet.measurement.gcloud.common.apiFuture
import org.wfanet.measurement.gcloud.common.await

typealias TransactionWork<R> = suspend (txn: AsyncDatabaseClient.TransactionContext) -> R

/**
 * Non-blocking wrapper around [dbClient] using asynchronous Spanner Java API.
 *
 * This class only exposes the methods used by this project and not the complete API.
 *
 * @param dbClient [DatabaseClient] to wrap
 * @param executor [Executor] for read-write transactions
 * @param transactionTimeout timeout duration for read-write transactions
 */
class AsyncDatabaseClient(
  private val dbClient: DatabaseClient,
  private val executor: Executor,
  /**
   * Timeout duration for read-write transactions.
   *
   * The Spanner client library will retry aborted transactions for up to 24 hours. We use a shorter
   * timeout to prevent that.
   *
   * Note that if transactions are running in an RPC context, the RPC deadline should probably be
   * shorter than this anyway.
   */
  private val transactionTimeout: Duration
) {
  /** @see DatabaseClient.singleUse */
  fun singleUse(bound: TimestampBound = TimestampBound.strong()): ReadContext {
    return ReadContextImpl(dbClient.singleUse(bound))
  }

  /** @see DatabaseClient.readOnlyTransaction */
  fun readOnlyTransaction(bound: TimestampBound = TimestampBound.strong()): ReadContext {
    return ReadContextImpl(dbClient.readOnlyTransaction(bound))
  }

  /** @see DatabaseClient.readWriteTransaction */
  fun readWriteTransaction(): TransactionRunner {
    return TransactionRunnerImpl(dbClient.runAsync())
  }

  /** @see DatabaseClient.write */
  suspend fun write(mutations: Iterable<Mutation>) {
    readWriteTransaction().execute { txn -> txn.buffer(mutations) }
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
    withTimeout(timeout.toMillis()) {
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
      vararg options: ReadOption
    ): Flow<Struct>

    /** @see com.google.cloud.spanner.ReadContext.readRow */
    suspend fun readRow(table: String, key: Key, columns: Iterable<String>): Struct?

    /** @see com.google.cloud.spanner.ReadContext.readRowUsingIndex */
    suspend fun readRowUsingIndex(
      table: String,
      index: String,
      key: Key,
      columns: Iterable<String>
    ): Struct?

    /** @see com.google.cloud.spanner.ReadContext.readRowUsingIndex */
    suspend fun readRowUsingIndex(
      table: String,
      index: String,
      key: Key,
      vararg columns: String
    ): Struct?

    /** @see com.google.cloud.spanner.ReadContext.executeQuery */
    fun executeQuery(statement: Statement, vararg options: QueryOption): Flow<Struct>
  }

  /** Coroutine version of [AsyncRunner]. */
  interface TransactionRunner {
    /** @see com.google.cloud.spanner.AsyncRunner.runAsync */
    suspend fun <R> execute(doWork: TransactionWork<R>): R

    suspend fun getCommitTimestamp(): Timestamp
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

  private inner class TransactionRunnerImpl(private val runner: AsyncRunner) : TransactionRunner {
    @OptIn(ExperimentalTime::class) // For `measureTimedValue`.
    override suspend fun <R> execute(doWork: TransactionWork<R>): R {
      try {
        val timedResult: TimedValue<R> = measureTimedValue {
          runner.run(executor, transactionTimeout) { txn -> doWork(TransactionContextImpl(txn)) }
        }
        logger.fine { "Transaction completed in ${timedResult.duration.inWholeMilliseconds}ms" }
        return timedResult.value
      } catch (e: SpannerException) {
        throw e.wrappedException ?: e
      }
    }

    override suspend fun getCommitTimestamp(): Timestamp {
      return runner.commitTimestamp.await()
    }
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
    vararg options: ReadOption
  ): Flow<Struct> {
    return readContext.readAsync(table, keys, columns, *options).asFlow()
  }

  override suspend fun readRow(table: String, key: Key, columns: Iterable<String>): Struct? {
    return readContext.readRowAsync(table, key, columns).await()
  }

  override suspend fun readRowUsingIndex(
    table: String,
    index: String,
    key: Key,
    columns: Iterable<String>
  ): Struct? {
    return readContext.readRowUsingIndexAsync(table, index, key, columns).await()
  }

  override suspend fun readRowUsingIndex(
    table: String,
    index: String,
    key: Key,
    vararg columns: String
  ): Struct? {
    return readRowUsingIndex(table, index, key, columns.asIterable())
  }

  override fun executeQuery(statement: Statement, vararg options: QueryOption): Flow<Struct> {
    return readContext.executeQueryAsync(statement, *options).asFlow()
  }
}

private class TransactionContextImpl(private val txn: TransactionContext) :
  AsyncDatabaseClient.TransactionContext, AsyncDatabaseClient.ReadContext by ReadContextImpl(txn) {

  override fun buffer(mutation: Mutation) = txn.buffer(mutation)

  override fun buffer(mutations: Iterable<Mutation>) = txn.buffer(mutations)

  override suspend fun executeUpdate(statement: Statement): Long {
    return txn.executeUpdateAsync(statement).await()
  }
}

/** Produces a [Flow] from the results in this [AsyncResultSet]. */
@OptIn(ExperimentalTime::class) // For `TimeSource`.
private fun AsyncResultSet.asFlow(): Flow<Struct> {
  val start = TimeSource.Monotonic.markNow()
  return callbackFlow<Struct> {
      fun resumeWhenReady(row: Struct) {
        launch(CoroutineName(::resumeWhenReady.name)) {
          send(row)
          resume()
        }
      }

      setCallback(DirectExecutor) { cursor ->
        val cursorState =
          try {
            cursor.tryNext()
          } catch (e: Exception) {
            if (e !is SpannerException) {
              AsyncDatabaseClient.logger.log(Level.SEVERE, e) {
                "Unexpected exception type in Spanner callback"
              }
            }
            close(e)
            return@setCallback AsyncResultSet.CallbackResponse.DONE
          }

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (cursorState) {
          AsyncResultSet.CursorState.OK -> {
            val sendResult = trySend(cursor.currentRowAsStruct)
            if (sendResult.isSuccess) {
              AsyncResultSet.CallbackResponse.CONTINUE
            } else if (sendResult.isFailure) {
              resumeWhenReady(cursor.currentRowAsStruct)
              AsyncResultSet.CallbackResponse.PAUSE
            } else {
              close(sendResult.exceptionOrNull())
              AsyncResultSet.CallbackResponse.DONE
            }
          }
          AsyncResultSet.CursorState.NOT_READY -> AsyncResultSet.CallbackResponse.CONTINUE
          AsyncResultSet.CursorState.DONE -> {
            close()
            AsyncResultSet.CallbackResponse.DONE
          }
        }
      }

      awaitClose {
        if (!coroutineContext.isActive) {
          this@asFlow.cancel()
        }

        // Close AsyncResultSet.
        this@asFlow.close()
      }
    }
    .onCompletion { cause ->
      // For some reason, having an onCompletion specified here is significant. Without it,
      // sometimes the collector will hang. See
      // https://github.com/Kotlin/kotlinx.coroutines/issues/3459

      AsyncDatabaseClient.logger.log(Level.FINE, cause) {
        "Flow completed in ${start.elapsedNow().inWholeMilliseconds}ms"
      }
    }
    .buffer(Channel.RENDEZVOUS)
}

private suspend fun <T> AsyncRunner.run(
  executor: Executor,
  transactionTimeout: Duration,
  doWork: suspend (TransactionContext) -> T
): T {
  // The Spanner client library may call the async callback multiple times as it performs retries on
  // certain exceptions. Therefore, we use supervisorScope to prevent these exceptions from
  // cancelling the parent job.
  return supervisorScope {
    val txnFuture: ApiFuture<T> = runAsync({ txn -> apiFuture { doWork(txn) } }, executor)

    withTimeout(transactionTimeout) {
      // Wait inside the coroutine scope to ensure any callback futures are created before the scope
      // completes, and also propagate cancellation to child coroutines.
      txnFuture.await()
    }
  }
}

fun Spanner.getAsyncDatabaseClient(
  databaseId: DatabaseId,
  executor: Executor = Executors.newSingleThreadExecutor(),
  transactionTimeout: Duration = Duration.ofMinutes(1L)
) = AsyncDatabaseClient(getDatabaseClient(databaseId), executor, transactionTimeout)

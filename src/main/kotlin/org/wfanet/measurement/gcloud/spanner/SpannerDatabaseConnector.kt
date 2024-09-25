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

import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import io.opentelemetry.api.metrics.Meter
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.TimeoutCancellationException
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.ThreadPoolInstrumentation

/**
 * Wraps a connection to a Spanner database for convenient access to an [AsyncDatabaseClient], the
 * [DatabaseId], waiting for the connection to be ready, etc.
 */
class SpannerDatabaseConnector(
  projectName: String,
  instanceName: String,
  databaseName: String,
  private val readyTimeout: Duration,
  maxTransactionThreads: Int,
  emulatorHost: String?,
) : AutoCloseable {
  init {
    require(maxTransactionThreads > 0)
  }

  private val spanner: Spanner =
    buildSpanner(projectName, emulatorHost).also {
      Runtime.getRuntime()
        .addShutdownHook(
          Thread {
            System.err.println("Ensuring Spanner is closed...")
            if (!it.isClosed) {
              it.close()
            }
            System.err.println("Spanner closed")
          }
        )
    }
  private val transactionExecutor: Lazy<ExecutorService> = lazy {
    if (emulatorHost == null) {
      ThreadPoolExecutor(1, maxTransactionThreads, 60L, TimeUnit.SECONDS, LinkedBlockingQueue())
        .also { threadPoolInstrumentation.registerThreadPool(databaseId.name, it) }
    } else {
      // Spanner emulator only supports a single read-write transaction at a time.
      Executors.newSingleThreadExecutor()
    }
  }

  val databaseId: DatabaseId = DatabaseId.of(projectName, instanceName, databaseName)
  val databaseClient: AsyncDatabaseClient by lazy {
    spanner.getAsyncDatabaseClient(databaseId, transactionExecutor.value)
  }

  /**
   * Suspends until [databaseClient] is ready, throwing a
   * [kotlinx.coroutines.TimeoutCancellationException] if [readyTimeout] is reached.
   */
  suspend fun waitUntilReady() {
    databaseClient.waitUntilReady(readyTimeout)
  }

  override fun close() {
    spanner.close()
    if (transactionExecutor.isInitialized()) {
      threadPoolInstrumentation.unregisterThreadPool(databaseId.name)
      transactionExecutor.value.shutdown()
    }
  }

  /**
   * Executes [block] with this [SpannerDatabaseConnector] once it's ready, ensuring that this is
   * closed when done.
   */
  suspend fun <R> usingSpanner(block: suspend (spanner: SpannerDatabaseConnector) -> R): R {
    use { spanner ->
      try {
        logger.info { "Waiting for Spanner connection to $databaseId to be ready..." }
        spanner.waitUntilReady()
        logger.info { "Spanner connection to $databaseId ready" }
      } catch (e: TimeoutCancellationException) {
        // Closing Spanner can take a long time (e.g. 1 minute) and delay the
        // exception being surfaced, so we log here to give immediate feedback.
        logger.severe { "Timed out waiting for Spanner to be ready" }
        throw e
      }
      return block(spanner)
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
    private val meter: Meter = Instrumentation.getMeter(this::class.java.name)
    private val threadPoolInstrumentation =
      ThreadPoolInstrumentation(
        meter,
        "${Instrumentation.ROOT_NAMESPACE}.spanner.transaction.thread_pool"
      )
  }
}

/** Builds a [SpannerDatabaseConnector] from these flags. */
private fun SpannerFlags.toSpannerDatabaseConnector(): SpannerDatabaseConnector {
  return SpannerDatabaseConnector(
    projectName = projectName,
    instanceName = instanceName,
    databaseName = databaseName,
    readyTimeout = readyTimeout,
    maxTransactionThreads = maxTransactionThreads,
    emulatorHost = emulatorHost,
  )
}

/**
 * Executes [block] with a [SpannerDatabaseConnector] resource once it's ready, ensuring that the
 * resource is closed.
 */
suspend fun <R> SpannerFlags.usingSpanner(
  block: suspend (spanner: SpannerDatabaseConnector) -> R
): R {
  return toSpannerDatabaseConnector().usingSpanner(block)
}

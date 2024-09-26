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

package org.wfanet.measurement.common

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor

/** Common instrumentation for an application. */
object Instrumentation {
  /** Root namespace. */
  const val ROOT_NAMESPACE = "halo_cmm"

  /** Singleton [OpenTelemetry] instance which may be initialized by the Java agent. */
  val openTelemetry: OpenTelemetry by lazy {
    // Using lazy delegate to avoid accessing global instance before it's initialized.
    GlobalOpenTelemetry.get()
  }

  /** Common [Meter]. */
  val meter: Meter by lazy { openTelemetry.getMeter(this::class.java.name) }

  /** Instrumentation for thread pools. */
  private val threadPools: ThreadPools by lazy { ThreadPools() }

  /**
   * Instruments the specified thread pool.
   *
   * @return the instrumented thread pool executor service
   */
  fun instrumentThreadPool(poolName: String, threadPool: ThreadPoolExecutor): ExecutorService {
    return threadPools.instrument(poolName, threadPool)
  }

  private class ThreadPools {
    private val threadPoolsByName = ConcurrentHashMap<String, ThreadPoolExecutor>()

    private val sizeCounter =
      meter
        .upDownCounterBuilder("$NAMESPACE.size")
        .setDescription("Current number of threads")
        .buildObserver()
    private val activeCounter =
      meter
        .upDownCounterBuilder("$NAMESPACE.active_count")
        .setDescription("Approximate number of threads that are actively executing tasks")
        .buildObserver()

    init {
      meter.batchCallback(::record, sizeCounter, activeCounter)
    }

    /** Registers the specified thread pool for instrumentation. */
    fun instrument(poolName: String, threadPool: ThreadPoolExecutor): ExecutorService {
      val previousRegistration = threadPoolsByName.putIfAbsent(poolName, threadPool)
      check(previousRegistration == null) { "Thread pool $poolName already instrumented" }
      return InstrumentedExecutorService(poolName, threadPool)
    }

    private fun record() {
      for ((poolName, threadPool) in threadPoolsByName) {
        val attributes: Attributes = Attributes.of(THREAD_POOL_NAME_ATTRIBUTE_KEY, poolName)
        sizeCounter.record(threadPool.poolSize.toLong(), attributes)
        activeCounter.record(threadPool.activeCount.toLong(), attributes)
      }
    }

    /** Instrumented [ExecutorService] */
    private inner class InstrumentedExecutorService(
      private val poolName: String,
      private val delegate: ExecutorService,
    ) : ExecutorService by delegate {
      override fun shutdown() {
        threadPoolsByName.remove(poolName)
        delegate.shutdown()
      }

      override fun shutdownNow(): MutableList<Runnable> {
        threadPoolsByName.remove(poolName)
        return delegate.shutdownNow()
      }
    }

    companion object {
      private const val NAMESPACE = "$ROOT_NAMESPACE.thread_pool"
      /** Attribute key for thread pool name. */
      private val THREAD_POOL_NAME_ATTRIBUTE_KEY: AttributeKey<String> =
        AttributeKey.stringKey("$NAMESPACE.name")
    }
  }
}

/**
 * Instruments the [ThreadPoolExecutor].
 *
 * @return the instrumented [ExecutorService]
 */
fun ThreadPoolExecutor.instrumented(poolName: String): ExecutorService =
  Instrumentation.instrumentThreadPool(poolName, this)

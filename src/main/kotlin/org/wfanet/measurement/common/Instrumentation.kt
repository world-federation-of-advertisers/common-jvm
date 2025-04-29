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
import io.opentelemetry.api.metrics.BatchCallback
import io.opentelemetry.api.metrics.Meter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadPoolExecutor

interface Instrumentation {
  /** Instrumentation handle which cleans up instrumentation on [close]. */
  interface Handle : AutoCloseable

  /** Singleton [OpenTelemetry] instance which may be initialized by the Java agent. */
  val openTelemetry: OpenTelemetry

  /** Common [Meter]. */
  val meter: Meter

  /**
   * Instruments [threadPool] as [poolName].
   *
   * @return a [Handle] for cleaning up instrumentation
   * @throws IllegalStateException if a thread pool with [poolName] has already been instrumented by
   *   this instance
   */
  fun instrumentThreadPool(poolName: String, threadPool: ThreadPoolExecutor): Handle

  companion object : Instrumentation {
    /** Root namespace. */
    const val ROOT_NAMESPACE = "halo_cmm"

    private val instance = invalidatableLazy {
      // Delayed instantiation to avoid accessing global OpenTelemetry instance before it's ready.
      InstrumentationImpl(GlobalOpenTelemetry.get())
    }

    override val openTelemetry: OpenTelemetry
      get() = instance.value.openTelemetry

    override val meter: Meter
      get() = instance.value.meter

    override fun instrumentThreadPool(poolName: String, threadPool: ThreadPoolExecutor) =
      instance.value.instrumentThreadPool(poolName, threadPool)

    /**
     * Resets [Instrumentation].
     *
     * This should only be used for tests.
     */
    fun resetForTest() {
      if (!instance.isInitialized()) {
        return
      }

      instance.value.close()
      instance.invalidate()
    }
  }
}

private class InstrumentationImpl(override val openTelemetry: OpenTelemetry) :
  Instrumentation, AutoCloseable {
  override val meter: Meter = openTelemetry.getMeter(this::class.java.name)

  private val threadPools = ThreadPools(meter)

  override fun instrumentThreadPool(
    poolName: String,
    threadPool: ThreadPoolExecutor,
  ): Instrumentation.Handle = threadPools.instrument(poolName, threadPool)

  override fun close() {
    threadPools.close()
  }

  private class ThreadPools(meter: Meter) : AutoCloseable {
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
    private val batchCallback: BatchCallback =
      meter.batchCallback(::record, sizeCounter, activeCounter)

    /** Registers the specified thread pool for instrumentation. */
    fun instrument(poolName: String, threadPool: ThreadPoolExecutor): Instrumentation.Handle {
      val previousRegistration = threadPoolsByName.putIfAbsent(poolName, threadPool)
      check(previousRegistration == null) { "Thread pool $poolName already instrumented" }
      return ThreadPoolHandle(poolName)
    }

    private fun record() {
      for ((poolName, threadPool) in threadPoolsByName) {
        val attributes: Attributes = Attributes.of(THREAD_POOL_NAME_ATTRIBUTE_KEY, poolName)
        sizeCounter.record(threadPool.poolSize.toLong(), attributes)
        activeCounter.record(threadPool.activeCount.toLong(), attributes)
      }
    }

    override fun close() {
      batchCallback.close()
      threadPoolsByName.clear()
    }

    inner class ThreadPoolHandle(private val poolName: String) : Instrumentation.Handle {
      override fun close() {
        threadPoolsByName.remove(poolName)
      }
    }

    companion object {
      private const val NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.thread_pool"
      /** Attribute key for thread pool name. */
      private val THREAD_POOL_NAME_ATTRIBUTE_KEY: AttributeKey<String> =
        AttributeKey.stringKey("$NAMESPACE.name")
    }
  }
}

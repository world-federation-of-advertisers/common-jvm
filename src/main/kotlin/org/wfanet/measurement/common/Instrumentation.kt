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
import java.util.concurrent.ThreadPoolExecutor

/**
 * Common instrumentation for an application.
 *
 * This provides the singleton [OpenTelemetry] instance which may be initialized by the Java agent.
 */
object Instrumentation : OpenTelemetry by GlobalOpenTelemetry.get() {
  /** Root namespace. */
  const val ROOT_NAMESPACE = "halo_cmm"
}

/** Instrumentation for a thread pool. */
class ThreadPoolInstrumentation(meter: Meter, namespace: String) {
  private val threadPoolsByName = ConcurrentHashMap<String, ThreadPoolExecutor>()

  private val sizeCounter =
    meter
      .upDownCounterBuilder("${namespace}.size")
      .setDescription("Current number of threads")
      .buildObserver()
  private val activeCounter =
    meter
      .upDownCounterBuilder("${namespace}.active_count")
      .setDescription("Approximate number of threads that are actively executing tasks")
      .buildObserver()

  init {
    meter.batchCallback(::record, sizeCounter, activeCounter)
  }

  /** Registers the specified thread pool for instrumentation. */
  fun registerThreadPool(poolName: String, threadPool: ThreadPoolExecutor) {
    val previousRegistration = threadPoolsByName.putIfAbsent(poolName, threadPool)
    check(previousRegistration == null) { "Thread pool $poolName already registered" }
  }

  /** Unregisters the specified thread pool for instrumentation. */
  fun unregisterThreadPool(poolName: String) = threadPoolsByName.remove(poolName)

  private fun record() {
    for ((poolName, threadPool) in threadPoolsByName) {
      val attributes: Attributes = Attributes.of(THREAD_POOL_NAME_ATTRIBUTE_KEY, poolName)
      sizeCounter.record(threadPool.poolSize.toLong(), attributes)
      activeCounter.record(threadPool.activeCount.toLong(), attributes)
    }
  }

  companion object {
    /** Attribute key for thread pool name. */
    private val THREAD_POOL_NAME_ATTRIBUTE_KEY: AttributeKey<String> =
      AttributeKey.stringKey("${Instrumentation.ROOT_NAMESPACE}.thread_pool.name")
  }
}

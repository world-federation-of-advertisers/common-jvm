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

import com.google.common.truth.Truth.assertThat
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class InstrumentationTest {
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader

  @Before
  fun initOpenTelemetry() {
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .buildAndRegisterGlobal()
  }

  @After
  fun resetOpenTelemetry() {
    SdkMeterProviderUtil.resetForTest(openTelemetry.sdkMeterProvider)
  }

  @Test
  fun `instrumented instruments thread pool`() {
    val poolName = "thread-pool"
    val poolSize = 2
    val executor =
      ThreadPoolExecutor(poolSize, poolSize, 1L, TimeUnit.DAYS, LinkedBlockingQueue())
        .instrumented(poolName)
    // Guarantee that executor is shut down by end of test method.
    AutoCloseable { executor.shutdownNow() }
      .use {
        repeat(poolSize) {
          executor.execute {
            // No-op
          }
        }
        executor.execute { Thread.sleep(Long.MAX_VALUE) }

        // Trigger recording of values.
        metricReader.forceFlush()
      }

    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(2)
    val pointValuesByMetricName: Map<String, List<Long>> =
      metricData.associateBy({ it.name }, { it.longSumData.points.map { point -> point.value } })
    assertThat(pointValuesByMetricName.keys)
      .containsExactly(POOL_SIZE_METRIC_NAME, ACTIVE_COUNT_METRIC_NAME)
    assertThat(pointValuesByMetricName.getValue(POOL_SIZE_METRIC_NAME))
      .containsExactly(poolSize.toLong())
    assertThat(pointValuesByMetricName.getValue(ACTIVE_COUNT_METRIC_NAME)).containsExactly(1L)
  }

  @Test
  fun `instrumented does not record after shut down`() {
    val poolName = "thread-pool"
    val poolSize = 2
    val executor =
      ThreadPoolExecutor(poolSize, poolSize, 1L, TimeUnit.DAYS, LinkedBlockingQueue())
        .instrumented(poolName)
    AutoCloseable { executor.shutdownNow() }
      .use {
        repeat(poolSize) {
          executor.execute {
            // No-op
          }
        }
      }

    // Trigger recording of values after shutdown.
    metricReader.forceFlush()

    assertThat(metricExporter.finishedMetricItems).isEmpty()
  }

  @Test
  fun `instrumented throws on duplicate pool name`() {
    val poolName = "thread-pool"
    val poolSize = 2
    ThreadPoolExecutor(poolSize, poolSize, 1L, TimeUnit.DAYS, LinkedBlockingQueue())
      .instrumented(poolName)

    assertFailsWith<IllegalStateException> {
      ThreadPoolExecutor(poolSize, poolSize, 1L, TimeUnit.DAYS, LinkedBlockingQueue())
        .instrumented(poolName)
    }
  }

  companion object {
    private const val POOL_SIZE_METRIC_NAME = "halo_cmm.thread_pool.size"
    private const val ACTIVE_COUNT_METRIC_NAME = "halo_cmm.thread_pool.active_count"
  }
}

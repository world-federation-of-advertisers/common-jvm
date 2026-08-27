/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import com.google.common.truth.Truth.assertThat
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.StatusException
import io.netty.handler.ssl.ClientAuth
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.FakeRequest
import org.wfanet.measurement.common.FakeResponse
import org.wfanet.measurement.common.FakeServiceGrpcKt

@RunWith(JUnit4::class)
class CommonServerOverloadTest {
  private val startedLatch = CountDownLatch(1)
  private val releaseLatch = CountDownLatch(1)

  // corePoolSize=0, maximumPoolSize=1, SynchronousQueue: no capacity to hold a task beyond the
  // one thread, so a second concurrent call is rejected rather than queued.
  private val executor =
    ThreadPoolExecutor(
      0,
      1,
      60L,
      TimeUnit.SECONDS,
      SynchronousQueue(),
      Executors.defaultThreadFactory(),
    )

  private val service =
    object : FakeServiceGrpcKt.FakeServiceCoroutineImplBase(executor.asCoroutineDispatcher()) {
      override suspend fun fake(requests: kotlinx.coroutines.flow.Flow<FakeRequest>): FakeResponse {
        startedLatch.countDown()
        releaseLatch.await()
        return FakeResponse.getDefaultInstance()
      }
    }

  private val server: CommonServer =
    CommonServer.fromParameters(
        verboseGrpcLogging = false,
        certs = null,
        clientAuth = ClientAuth.NONE,
        nameForLogging = "CommonServerOverloadTest",
        services = listOf(service.bindService()),
        executor = executor,
      )
      .start()

  private val channel: ManagedChannel =
    ManagedChannelBuilder.forAddress("localhost", server.port).usePlaintext().build()

  @After
  fun tearDown() {
    releaseLatch.countDown()
    channel.shutdownNow()
    server.close()
  }

  @Test
  fun `saturated executor closes call with RESOURCE_EXHAUSTED rather than hanging`() = runBlocking {
    val stub = FakeServiceGrpcKt.FakeServiceCoroutineStub(channel)

    // Occupy the sole executor thread with an in-flight call.
    val holderJob =
      launch(Dispatchers.IO) {
        runCatching {
          stub
            .withDeadlineAfter(60, TimeUnit.SECONDS)
            .fake(flowOf(FakeRequest.getDefaultInstance()))
        }
      }
    assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue()

    // A second call must dispatch through the same saturated, zero-capacity-queue executor.
    val start = System.nanoTime()
    val thrown =
      assertFailsWith<StatusException> {
        withTimeout(5_000) {
          stub.withDeadlineAfter(5, TimeUnit.SECONDS).fake(flowOf(FakeRequest.getDefaultInstance()))
        }
      }
    val elapsedMillis = (System.nanoTime() - start) / 1_000_000

    assertThat(thrown.status.code).isEqualTo(Status.Code.RESOURCE_EXHAUSTED)
    // Should fail promptly on rejection, not wait out the 5s client deadline.
    assertThat(elapsedMillis).isLessThan(2_000)

    releaseLatch.countDown()
    holderJob.join()
  }

  @Test
  fun `unsaturated executor still serves calls normally`() = runBlocking {
    releaseLatch.countDown()
    val stub = FakeServiceGrpcKt.FakeServiceCoroutineStub(channel)
    val response = stub.fake(flowOf(FakeRequest.getDefaultInstance()))
    assertThat(response).isEqualTo(FakeResponse.getDefaultInstance())
  }

  @Test
  fun `shutdown executor closes call with UNAVAILABLE rather than RESOURCE_EXHAUSTED`() =
    runBlocking {
      releaseLatch.countDown()
      executor.shutdown()

      val stub = FakeServiceGrpcKt.FakeServiceCoroutineStub(channel)
      val thrown =
        assertFailsWith<StatusException> {
          withTimeout(5_000) {
            stub
              .withDeadlineAfter(5, TimeUnit.SECONDS)
              .fake(flowOf(FakeRequest.getDefaultInstance()))
          }
        }

      assertThat(thrown.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    }
}

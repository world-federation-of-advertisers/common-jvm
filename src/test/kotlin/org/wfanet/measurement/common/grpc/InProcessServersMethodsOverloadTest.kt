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
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
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
import picocli.CommandLine

/**
 * Verifies that [InProcessServersMethods.startInProcessServerWithService]'s optional executor
 * parameter actually installs overload protection -- this is a distinct code path from
 * [CommonServer], not covered by [CommonServerOverloadTest].
 */
@RunWith(JUnit4::class)
class InProcessServersMethodsOverloadTest {
  private val startedLatch = CountDownLatch(1)
  private val releaseLatch = CountDownLatch(1)

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

  private val serverName = InProcessServerBuilder.generateName()

  private val commonServerFlags = CommonServer.Flags().apply { CommandLine(this).parseArgs() }

  private val channel: ManagedChannel =
    InProcessChannelBuilder.forName(serverName).directExecutor().build()

  @After
  fun tearDown() {
    releaseLatch.countDown()
    channel.shutdownNow()
  }

  @Test
  fun `saturated executor closes in-process call with RESOURCE_EXHAUSTED`() = runBlocking {
    InProcessServersMethods.startInProcessServerWithService(
      serverName,
      commonServerFlags,
      service.bindService(),
      executor,
    )

    val stub = FakeServiceGrpcKt.FakeServiceCoroutineStub(channel)

    val holderJob =
      launch(Dispatchers.IO) {
        runCatching {
          stub
            .withDeadlineAfter(60, TimeUnit.SECONDS)
            .fake(flowOf(FakeRequest.getDefaultInstance()))
        }
      }
    assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue()

    val thrown =
      assertFailsWith<StatusException> {
        withTimeout(5_000) {
          stub.withDeadlineAfter(5, TimeUnit.SECONDS).fake(flowOf(FakeRequest.getDefaultInstance()))
        }
      }

    assertThat(thrown.status.code).isEqualTo(Status.Code.RESOURCE_EXHAUSTED)

    releaseLatch.countDown()
    holderJob.join()
  }
}

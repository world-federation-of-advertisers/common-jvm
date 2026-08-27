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
import kotlin.coroutines.resume
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.FakeRequest
import org.wfanet.measurement.common.FakeResponse
import org.wfanet.measurement.common.FakeServiceGrpcKt
import org.wfanet.measurement.common.fakeRequest

/**
 * Verifies that a rejection on a *resumption* dispatch (RPC already active, suspended and
 * redispatching) closes with `INTERNAL`, not the `RESOURCE_EXHAUSTED` used for the *initial*
 * dispatch case covered by [CommonServerOverloadTest] -- the RPC's handler has already run some
 * code by this point, so it would be unsafe to imply the whole RPC can simply be retried. Without
 * OverloadAwareServerInterceptor, this scenario surfaces as `CANCELLED` instead (confirmed via a
 * separate scratch investigation) -- distinct from the initial-dispatch case, which just hangs to
 * deadline.
 */
@RunWith(JUnit4::class)
class CommonServerNestedRejectionTest {
  private val aStartedLatch = CountDownLatch(1)
  private var aPendingResume: CancellableContinuation<Unit>? = null
  private val bStartedLatch = CountDownLatch(1)
  private val bReleaseLatch = CountDownLatch(1)

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
        val request = requests.first()
        when (request.number) {
          1 -> {
            // Call A: initial dispatch succeeds, then suspends -- releasing the sole thread --
            // and must be redispatched through the same executor to resume. The continuation is
            // captured, and the latch only counted down, once this suspension is genuinely
            // committed -- unlike a CompletableDeferred the test completes, there's no window
            // where resuming races ahead of the suspend actually taking effect and turns into a
            // same-thread no-op instead of a real redispatch.
            suspendCancellableCoroutine<Unit> { cont ->
              aPendingResume = cont
              aStartedLatch.countDown()
            }
          }
          2 -> {
            // Call B: occupies the (now-free) sole thread until released, so it's still running
            // when A's resumption dispatch is triggered.
            bStartedLatch.countDown()
            bReleaseLatch.await()
          }
        }
        return FakeResponse.getDefaultInstance()
      }
    }

  private val server: CommonServer =
    CommonServer.fromParameters(
        verboseGrpcLogging = false,
        certs = null,
        clientAuth = ClientAuth.NONE,
        nameForLogging = "CommonServerNestedRejectionTest",
        services = listOf(service.bindService()),
        executor = executor,
      )
      .start()

  private val channel: ManagedChannel =
    ManagedChannelBuilder.forAddress("localhost", server.port).usePlaintext().build()

  @After
  fun tearDown() {
    bReleaseLatch.countDown()
    channel.shutdownNow()
    server.close()
  }

  @Test
  fun `rejected resumption dispatch closes with INTERNAL not RESOURCE_EXHAUSTED`() = runBlocking {
    val stub = FakeServiceGrpcKt.FakeServiceCoroutineStub(channel)

    supervisorScope {
      val aDeferred =
        async(Dispatchers.IO) {
          stub.withDeadlineAfter(30, TimeUnit.SECONDS).fake(flowOf(fakeRequest { number = 1 }))
        }
      assertThat(aStartedLatch.await(5, TimeUnit.SECONDS)).isTrue()

      val bJob =
        launch(Dispatchers.IO) {
          runCatching {
            stub.withDeadlineAfter(30, TimeUnit.SECONDS).fake(flowOf(fakeRequest { number = 2 }))
          }
        }
      assertThat(bStartedLatch.await(5, TimeUnit.SECONDS)).isTrue()

      // B has deterministically acquired the sole worker thread -- only now trigger A's
      // resumption dispatch, guaranteeing it lands while B still holds the thread, with no race
      // on timing. A tight withTimeout paired with a much longer RPC deadline distinguishes a
      // prompt clean failure from a hang: a hang would blow through withTimeout and throw
      // TimeoutCancellationException instead of StatusException, failing this assertion.
      aPendingResume!!.resume(Unit)
      val thrown = assertFailsWith<StatusException> { withTimeout(3_000) { aDeferred.await() } }
      assertThat(thrown.status.code).isEqualTo(Status.Code.INTERNAL)

      bReleaseLatch.countDown()
      bJob.join()
    }
  }

  @Test
  fun `resumption rejected by shutdown closes with INTERNAL not UNAVAILABLE`() = runBlocking {
    val stub = FakeServiceGrpcKt.FakeServiceCoroutineStub(channel)

    supervisorScope {
      val aDeferred =
        async(Dispatchers.IO) {
          stub.withDeadlineAfter(30, TimeUnit.SECONDS).fake(flowOf(fakeRequest { number = 1 }))
        }
      assertThat(aStartedLatch.await(5, TimeUnit.SECONDS)).isTrue()

      // The RPC has already started (and, in a real handler, could have already performed a
      // non-idempotent side effect) before the executor shuts down out from under its
      // resumption -- this must not be conflated with a clean pre-start shutdown, which is safe
      // to call UNAVAILABLE.
      executor.shutdown()
      aPendingResume!!.resume(Unit)

      val thrown = assertFailsWith<StatusException> { withTimeout(3_000) { aDeferred.await() } }
      assertThat(thrown.status.code).isEqualTo(Status.Code.INTERNAL)
    }
  }
}

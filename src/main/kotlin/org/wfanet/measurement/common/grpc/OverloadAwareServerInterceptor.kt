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

import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.grpc.kotlin.CoroutineContextServerInterceptor
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job

/**
 * [ServerInterceptor] that surfaces rejection of a coroutine-service [Executor] dispatch to the
 * client as a clean gRPC status instead of leaving the RPC to hang until its deadline.
 *
 * Coroutine services dispatched via an [Executor]-backed [CoroutineDispatcher] (e.g.
 * `executor.asCoroutineDispatcher()`) already have kotlinx-coroutines' own safety net for a
 * [RejectedExecutionException]: it cancels the coroutine's [Job] and completes bookkeeping on
 * [Dispatchers.IO]. But nothing in that path closes the underlying [ServerCall], so the client
 * simply waits until its own deadline expires. This interceptor overrides the per-RPC
 * [CoroutineContext] with a dispatcher that closes the call itself, immediately, on rejection.
 *
 * Must be paired with [CloseOnceServerInterceptor] ordered ahead of this interceptor in the chain
 * (i.e. added *after* this one, since the last-added interceptor runs first): gRPC does not
 * tolerate a [ServerCall] being closed twice, and the coroutine's own completion handling will
 * still attempt to close the call with `CANCELLED` after this interceptor has already closed it.
 *
 * The status chosen depends on whether the RPC's handler code has already started running by the
 * time of the rejection, since that determines whether it's safe to imply the request can simply be
 * retried:
 * - Rejected before the RPC ever started (e.g. a bounded queue at capacity): `RESOURCE_EXHAUSTED`.
 *   Nothing has run yet, so retrying is safe.
 * - Rejected before the RPC ever started, because [executor] is already shut down: `UNAVAILABLE`.
 *   Also safe to retry (e.g. against a different instance).
 * - Rejected on a *later* dispatch, after the RPC's handler already ran some code (e.g. a
 *   redispatch after resuming from a suspension point): `INTERNAL`, regardless of the reason for
 *   rejection. By this point the handler may already have performed a non-idempotent side effect,
 *   so it would be unsafe to suggest -- via a conventionally-retryable status -- that the client
 *   can simply retry the whole RPC.
 */
class OverloadAwareServerInterceptor(private val executor: Executor) :
  CoroutineContextServerInterceptor() {
  override fun coroutineContext(call: ServerCall<*, *>, headers: Metadata): CoroutineContext {
    return OverloadAwareDispatcher(executor, call)
  }

  private class OverloadAwareDispatcher(
    private val executor: Executor,
    private val call: ServerCall<*, *>,
  ) : CoroutineDispatcher() {
    private val rpcStarted = AtomicBoolean(false)

    override fun dispatch(context: CoroutineContext, block: Runnable) {
      val trackedBlock = Runnable {
        rpcStarted.set(true)
        block.run()
      }
      try {
        executor.execute(trackedBlock)
      } catch (e: RejectedExecutionException) {
        val status =
          if (rpcStarted.get()) {
            Status.INTERNAL.withDescription("Service executor rejected an already-admitted RPC")
          } else if ((executor as? ExecutorService)?.isShutdown == true) {
            Status.UNAVAILABLE.withDescription("Service executor is shut down")
          } else {
            Status.RESOURCE_EXHAUSTED.withDescription("Service executor rejected the task")
          }
        call.close(status.withCause(e), Metadata())
        context[Job]?.cancel(CancellationException("Rejected by executor", e))
        Dispatchers.IO.dispatch(context, block)
      }
    }
  }
}

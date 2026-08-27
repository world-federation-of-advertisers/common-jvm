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

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
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
 * [ServerInterceptor] that surfaces saturation of a coroutine-service [Executor] to the client as
 * `RESOURCE_EXHAUSTED` instead of leaving the RPC to hang until its deadline.
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
 * still attempt to close the call with `CANCELLED` after this interceptor has already closed it
 * with `RESOURCE_EXHAUSTED`.
 *
 * A rejection while [executor] is already shut down (e.g. during server shutdown) is distinct from
 * saturation under load, so it is surfaced as `UNAVAILABLE` instead.
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
    override fun dispatch(context: CoroutineContext, block: Runnable) {
      try {
        executor.execute(block)
      } catch (e: RejectedExecutionException) {
        val status =
          if ((executor as? ExecutorService)?.isShutdown == true) {
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

/**
 * [ServerInterceptor] that makes [ServerCall.close] idempotent, so a later close attempt (e.g. from
 * grpc-kotlin's own coroutine-completion handling after [OverloadAwareServerInterceptor] has
 * already closed the call) silently no-ops instead of throwing `IllegalStateException: call already
 * closed`.
 *
 * Must be ordered ahead of [OverloadAwareServerInterceptor] in the interceptor chain (i.e. added
 * *after* it, since the last-added interceptor runs first) so that interceptor's direct call to
 * `close()` goes through this wrapper.
 */
object CloseOnceServerInterceptor : ServerInterceptor {
  override fun <ReqT : Any, RespT : Any> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    return next.startCall(CloseOnceServerCall(call), headers)
  }

  private class CloseOnceServerCall<ReqT, RespT>(delegate: ServerCall<ReqT, RespT>) :
    SimpleForwardingServerCall<ReqT, RespT>(delegate) {
    private val closed = AtomicBoolean(false)

    override fun close(status: Status, trailers: Metadata) {
      if (closed.compareAndSet(false, true)) {
        super.close(status, trailers)
      }
    }
  }
}

// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.grpc.kotlin.GrpcContextElement
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch

/**
 * [ServerInterceptor] that can call suspending functions.
 *
 * TODO(grpc/grpc-kotlin#223): Replace with interceptor from grpc-kotlin once available.
 */
abstract class SuspendableServerInterceptor(
  private val coroutineContext: CoroutineContext = EmptyCoroutineContext
) : ServerInterceptor {

  override fun <ReqT : Any, RespT : Any> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    @Suppress("DEPRECATION") val deferredForwardingListener = DeferredForwardingListener<ReqT>()
    deferredForwardingListener.job =
      CoroutineScope(GrpcContextElement.current() + coroutineContext).launch {
        try {
          val delegate: ServerCall.Listener<ReqT> = interceptCallSuspending(call, headers, next)
          deferredForwardingListener.setDelegate(delegate)
        } catch (e: Exception) {
          if (!call.isCancelled) {
            val status =
              when (e) {
                is CancellationException -> Status.CANCELLED
                else -> Status.UNKNOWN
              }
            call.close(status.withCause(e), headers)
          }
        }
      }
    return deferredForwardingListener
  }

  /** @see interceptCall */
  abstract suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT>
}

/**
 * gRPC [ServerCall.Listener] that defers forwarding events to a delegate until [setDelegate] is
 * called.
 *
 * TODO(@SanjayVas): Make this private once it's not being used directly.
 */
@Deprecated(message = "Use SuspendableServerInterceptor rather than using this directly")
class DeferredForwardingListener<ReqT> : ServerCall.Listener<ReqT>() {
  private val ready = AtomicBoolean(false)
  private lateinit var delegate: ServerCall.Listener<ReqT>
  private var events = mutableListOf<Runnable>()

  var job: Job? = null

  override fun onMessage(message: ReqT) {
    maybeDeferEvent { delegate.onMessage(message) }
  }

  override fun onHalfClose() {
    maybeDeferEvent { delegate.onHalfClose() }
  }

  override fun onCancel() {
    try {
      maybeDeferEvent { delegate.onCancel() }
    } finally {
      job?.cancel()
    }
  }

  override fun onComplete() {
    maybeDeferEvent { delegate.onComplete() }
  }

  override fun onReady() {
    maybeDeferEvent { delegate.onReady() }
  }

  private fun maybeDeferEvent(event: Runnable) {
    if (ready.get()) {
      event.run()
      return
    }

    synchronized(this) {
      if (!ready.get()) { // Double-checked locking.
        events.add(event)
        return
      }
    }

    // Delegate was initialized outside the synchronized block, so we run immediately. This is done
    // outside the block to prevent deadlocks.
    event.run()
  }

  fun setDelegate(delegate: ServerCall.Listener<ReqT>) {
    check(!this::delegate.isInitialized)
    synchronized(this) {
      check(!this::delegate.isInitialized) // Double-checked locking.
      this.delegate = delegate
    }

    // We need to make sure all events get run, including ones that were added while the list is
    // being processed. Therefore, we run a loop until the list is empty.
    while (true) {
      var currentEvents = mutableListOf<Runnable>()
      synchronized(this) {
        if (events.isEmpty()) {
          ready.set(true)
          return
        }

        // Swap with a local list so that we can run events outside the synchronized block to
        // prevent deadlocks.
        val temp = currentEvents
        currentEvents = events
        events = temp
      }

      for (event in currentEvents) {
        event.run()
      }
      currentEvents.clear()
    }
  }
}

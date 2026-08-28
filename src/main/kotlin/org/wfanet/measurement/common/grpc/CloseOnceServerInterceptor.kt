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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger

/**
 * [ServerInterceptor] that makes [ServerCall.close] idempotent, since gRPC itself throws
 * `IllegalStateException` on a second call.
 *
 * Needed alongside [ExecutorRejectionServerInterceptor]: that interceptor closes the call directly
 * on a rejected dispatch, but the coroutine's own completion handling will still attempt to close
 * the same call afterward (e.g. with `CANCELLED`). This interceptor must be added *after*
 * [ExecutorRejectionServerInterceptor] in server setup, so that it runs first and wraps the call
 * before that interceptor's close call reaches it.
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
      } else {
        logger.log(Level.FINE) { "Suppressed duplicate ServerCall.close (status=$status)" }
      }
    }
  }

  private val logger: Logger = Logger.getLogger(this::class.java.name)
}

/*
 * Copyright 2022 The Cross-Media Measurement Authors
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
import java.util.logging.Level
import java.util.logging.Logger

/** [ServerInterceptor] that logs unexpected gRPC errors. */
object ErrorLoggingServerInterceptor : ServerInterceptor {
  override fun <ReqT : Any, RespT : Any> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val forwardingCall =
      object : SimpleForwardingServerCall<ReqT, RespT>(call) {
        override fun close(status: Status, trailers: Metadata) {
          when (status.code) {
            Status.Code.UNKNOWN,
            Status.Code.INTERNAL -> {
              val methodDescriptor = call.methodDescriptor
              val threadName = Thread.currentThread().name
              val message =
                listOfNotNull(
                    "[$threadName]",
                    "gRPC error:",
                    status.code.name,
                    status.description,
                  )
                  .joinToString(" ")
              logger.logp(
                Level.WARNING,
                methodDescriptor.serviceName,
                methodDescriptor.bareMethodName,
                message,
                status.cause
              )
            }
            else -> {}
          }
          super.close(status, trailers)
        }
      }

    return next.startCall(forwardingCall, headers)
  }

  private val logger: Logger = Logger.getLogger(this::class.java.name)
}

// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.protobuf.Message
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.truncateByteFields

/** Logs all gRPC requests and responses for clients. */
object LoggingClientInterceptor : ClientInterceptor {
  private const val BYTES_TO_LOG = 100
  private val logger: Logger = Logger.getLogger(this::class.java.name)
  private val threadName: String
    get() = Thread.currentThread().name

  private val requestCounter = AtomicLong()

  override fun <ReqT, RespT> interceptCall(
    method: MethodDescriptor<ReqT, RespT>,
    callOptions: CallOptions,
    next: Channel,
  ): ClientCall<ReqT, RespT> {
    val requestNumber = requestCounter.incrementAndGet()
    val requestId = Tracing.getOtelTraceId() ?: requestNumber.toString()
    val serviceName = method.serviceName
    val methodName = method.bareMethodName
    val nextCall = next.newCall(method, callOptions)

    return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(nextCall) {
      override fun start(responseListener: Listener<RespT>, headers: Metadata) {
        logger.logp(
          Level.INFO,
          serviceName,
          methodName,
          "[$threadName] gRPC client $requestId headers:\n$headers",
        )
        val listener =
          object : SimpleForwardingClientCallListener<RespT>(responseListener) {
            override fun onMessage(message: RespT) {
              val messageToLog = (message as Message).truncateByteFields(BYTES_TO_LOG)
              logger.logp(
                Level.INFO,
                serviceName,
                methodName,
                "[$threadName] gRPC client $requestId response:\n$messageToLog",
              )
              super.onMessage(message)
            }

            override fun onClose(status: Status, trailers: Metadata) {
              if (status.isOk) {
                logger.logp(
                  Level.INFO,
                  serviceName,
                  methodName,
                  "[$threadName] gRPC client $requestId complete",
                )
              } else {
                val message =
                  listOfNotNull(
                      "[$threadName]",
                      "gRPC client $requestId error:",
                      status.code.name,
                      status.description,
                    )
                    .joinToString(" ")
                logger.logp(Level.INFO, serviceName, methodName, message, status.cause)
              }
              super.onClose(status, trailers)
            }
          }
        super.start(listener, headers)
      }

      override fun sendMessage(message: ReqT) {
        val messageToLog = (message as Message).truncateByteFields(BYTES_TO_LOG)
        logger.logp(
          Level.INFO,
          serviceName,
          methodName,
          "[$threadName] gRPC client $requestId request: $messageToLog",
        )
        super.sendMessage(message)
      }
    }
  }
}

/** Psuedo-constructor for backwards-compatibility. */
@Deprecated("Use singleton object", ReplaceWith("LoggingClientInterceptor"))
fun LoggingClientInterceptor() = LoggingClientInterceptor

/**
 * Enables [LoggingClientInterceptor] on the returned [Channel].
 *
 * @param enable if true (the default), enables verbose logging
 */
fun Channel.withVerboseLogging(enable: Boolean = true): Channel {
  return if (enable) {
    ClientInterceptors.interceptForward(this, LoggingClientInterceptor)
  } else {
    this
  }
}

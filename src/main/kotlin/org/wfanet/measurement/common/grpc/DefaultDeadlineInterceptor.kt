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

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.MethodDescriptor
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * [ClientInterceptor] which sets a deadline after [duration] for any call that does not already
 * have a deadline set.
 */
class DefaultDeadlineInterceptor(private val duration: Long, private val timeUnit: TimeUnit) :
  ClientInterceptor {

  override fun <ReqT : Any, RespT : Any> interceptCall(
    method: MethodDescriptor<ReqT, RespT>,
    callOptions: CallOptions,
    next: Channel,
  ): ClientCall<ReqT, RespT> {
    val subCallOptions =
      if (callOptions.deadline == null) {
        callOptions.withDeadlineAfter(duration, timeUnit)
      } else {
        callOptions
      }
    return next.newCall(method, subCallOptions)
  }
}

fun Channel.withDefaultDeadline(duration: Long, timeUnit: TimeUnit): Channel =
  ClientInterceptors.interceptForward(this, DefaultDeadlineInterceptor(duration, timeUnit))

fun Channel.withDefaultDeadline(duration: Duration): Channel =
  ClientInterceptors.interceptForward(
    this,
    DefaultDeadlineInterceptor(duration.toMillis(), TimeUnit.MILLISECONDS),
  )

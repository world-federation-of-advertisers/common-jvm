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

package org.wfanet.measurement.gcloud.common

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Future
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future

object DirectExecutor : Executor {
  override fun execute(command: Runnable) = command.run()
}

/** Suspends until the [ApiFuture] completes. */
suspend fun <T> ApiFuture<T>.await(): T {
  return suspendCoroutine { cont ->
    val callback =
      object : ApiFutureCallback<T> {
        override fun onFailure(t: Throwable) {
          cont.resumeWithException(t)
        }

        override fun onSuccess(result: T) {
          cont.resume(result)
        }
      }
    ApiFutures.addCallback(this, callback, DirectExecutor)
  }
}

/** Starts a new coroutine as an [ApiFuture]. */
fun <T> CoroutineScope.apiFuture(block: suspend CoroutineScope.() -> T): ApiFuture<T> {
  return ForwardingApiFuture(future { block() })
}

private class ForwardingApiFuture<T>(private val delegate: CompletableFuture<T>) :
  ApiFuture<T>, Future<T> by delegate {

  override fun addListener(listener: Runnable, executor: Executor) {
    delegate.whenCompleteAsync({ _, _ -> listener.run() }, executor)
  }
}

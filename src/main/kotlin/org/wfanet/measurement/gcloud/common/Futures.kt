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
import com.google.common.util.concurrent.ForwardingListenableFuture.SimpleForwardingListenableFuture
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.Uninterruptibles
import java.util.concurrent.ExecutionException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.guava.asListenableFuture
import kotlinx.coroutines.suspendCancellableCoroutine

/**
 * Suspends until the [ApiFuture] completes.
 *
 * @see kotlinx.coroutines.guava.await
 */
suspend fun <T> ApiFuture<T>.await(): T {
  try {
    if (isDone) return Uninterruptibles.getUninterruptibly(this)
  } catch (e: ExecutionException) {
    throw e.cause!!
  }

  return suspendCancellableCoroutine { cont ->
    addListener(
      {
        if (isCancelled) {
          cont.cancel()
        } else {
          try {
            cont.resume(Uninterruptibles.getUninterruptibly(this))
          } catch (e: ExecutionException) {
            cont.resumeWithException(e.cause!!)
          }
        }
      },
      MoreExecutors.directExecutor()
    )
  }
}

fun <T> Deferred<T>.asApiFuture(): ApiFuture<T> = asListenableFuture().asApiFuture()

private class ListenableFutureAdapter<T>(delegate: ListenableFuture<T>) :
  SimpleForwardingListenableFuture<T>(delegate), ApiFuture<T>

private fun <T> ListenableFuture<T>.asApiFuture(): ApiFuture<T> = ListenableFutureAdapter(this)

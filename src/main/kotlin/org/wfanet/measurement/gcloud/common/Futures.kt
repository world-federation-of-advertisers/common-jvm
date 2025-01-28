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
import com.google.api.core.ForwardingApiFuture
import com.google.common.util.concurrent.ForwardingListenableFuture.SimpleForwardingListenableFuture
import com.google.common.util.concurrent.ListenableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.guava.asListenableFuture
import kotlinx.coroutines.guava.await
import org.wfanet.measurement.common.guava.awaitAsync

fun <T> Deferred<T>.asApiFuture(): ApiFuture<T> = asListenableFuture().asApiFuture()

suspend fun <T> ApiFuture<T>.await(): T = asListenableFuture().await()

private class ListenableFutureAdapter<T>(delegate: ListenableFuture<T>) :
  SimpleForwardingListenableFuture<T>(delegate), ApiFuture<T>

private fun <T> ListenableFuture<T>.asApiFuture(): ApiFuture<T> {
  @Suppress("UNCHECKED_CAST")
  return (this as? ApiFuture<T>) ?: ListenableFutureAdapter(this)
}

private class ApiFutureAdapter<T>(delegate: ApiFuture<T>) :
  ForwardingApiFuture<T>(delegate), ListenableFuture<T>

fun <T> ApiFuture<T>.asListenableFuture(): ListenableFuture<T> {
  @Suppress("UNCHECKED_CAST")
  return (this as? ListenableFuture<T>) ?: ApiFutureAdapter<T>(this)
}

/**
 * Returns a [Deferred] which observes the [ListenableFuture] returned by [init].
 *
 * This should only be used in cases where [await] cannot be immediately called on the [ApiFuture].
 *
 * @see org.wfanet.measurement.common.guava.awaitAsync
 */
inline fun <T> CoroutineScope.awaitAsync(crossinline init: () -> ApiFuture<T>): Deferred<T> =
  awaitAsync {
    init().asListenableFuture()
  }

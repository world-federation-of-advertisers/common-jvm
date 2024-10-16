/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.guava

import com.google.common.util.concurrent.ListenableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.guava.await

/**
 * Returns a [Deferred] which observes the [ListenableFuture] returned by [init].
 *
 * This is an alternative to [kotlinx.coroutines.guava.asDeferred] which better handles
 * cancellation. It should only be used in cases where [kotlinx.coroutines.guava.await] cannot be
 * immediately called on the [ListenableFuture].
 */
inline fun <T> CoroutineScope.awaitAsync(
  // Use crossinline to prevent suspending operations.
  crossinline init: () -> ListenableFuture<T>
): Deferred<T> {
  val future = init()
  val deferred = async { future.await() }
  deferred.invokeOnCompletion { future.cancel(false) }
  return deferred
}

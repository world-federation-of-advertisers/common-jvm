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

package org.wfanet.measurement.common

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future

/** Coroutine wrapper around [asyncCache] which loads values using [load]. */
class LoadingCache<K, V>(
  private val asyncCache: AsyncCache<K, V>,
  private val load: suspend (K) -> V?,
) {
  init {
    require(asyncCache !is AsyncLoadingCache) { "asyncCache is already a loading cache" }
  }

  /** Returns the value for [key], or `null` if no value could be loaded. */
  suspend fun get(key: K): V? = coroutineScope { getAsync(key).await() }

  /**
   * Returns the value for [key].
   *
   * @throws NoSuchElementException if no value could be loaded for [key]
   */
  suspend fun getValue(key: K): V {
    return get(key) ?: throw NoSuchElementException("No element with key $key")
  }

  private fun CoroutineScope.getAsync(key: K): CompletableFuture<V?> {
    return asyncCache.get(key) { k, executor ->
      future(executor.asCoroutineDispatcher()) { load(k) }
    }
  }
}

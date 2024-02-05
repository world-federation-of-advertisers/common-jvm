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

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test

/** Test for [LoadingCache]. */
class LoadingCacheTest {
  @Test
  fun `ctor throws IllegalArgumentException when asyncCache has loader`() {
    val asyncCache = Caffeine.newBuilder().buildAsync<String, String> { key -> "$key-value" }
    assertFailsWith<IllegalArgumentException> { LoadingCache(asyncCache) { "$it-value" } }
  }

  @Test
  fun `get rethrows exception from load`() {
    val asyncCache = Caffeine.newBuilder().buildAsync<String, String>()
    val loadingCache = LoadingCache(asyncCache) { error("Loading error") }
    assertFailsWith<IllegalStateException> { runBlocking { loadingCache.get("foo") } }
  }

  @Test
  fun `get returns loaded value`() {
    val asyncCache = Caffeine.newBuilder().buildAsync<String, String>()
    val loadingCache = LoadingCache(asyncCache) { key -> "$key-value" }

    val value: String? = runBlocking { loadingCache.get("foo") }

    assertThat(value).isEqualTo("foo-value")
  }

  @Test
  fun `getValue returns loaded value`() {
    val asyncCache = Caffeine.newBuilder().buildAsync<String, String>()
    val loadingCache = LoadingCache(asyncCache) { key -> "$key-value" }

    val value: String = runBlocking { loadingCache.getValue("foo") }

    assertThat(value).isEqualTo("foo-value")
  }

  @Test
  fun `get returns null when load returns null`() {
    val asyncCache = Caffeine.newBuilder().buildAsync<String, String>()
    val loadingCache = LoadingCache(asyncCache) { null }

    val value: String? = runBlocking { loadingCache.get("foo") }

    assertThat(value).isNull()
  }

  @Test
  fun `getValue throws NoSuchElementException when load returns null`() {
    val asyncCache = Caffeine.newBuilder().buildAsync<String, String>()
    val loadingCache = LoadingCache(asyncCache) { null }

    assertFailsWith<NoSuchElementException> { runBlocking { loadingCache.getValue("foo") } }
  }
}

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

import com.google.common.truth.Truth.assertThat
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class InvalidatableLazyTest {
  private val instanceCount = AtomicInteger()

  private val lazy: InvalidatableLazy<Int> = invalidatableLazy { instanceCount.incrementAndGet() }

  @Test
  fun `value calls initializer once`() {
    val initialValue: Int = lazy.value
    assertThat(initialValue).isEqualTo(1)
    assertThat(lazy.value).isEqualTo(initialValue)
  }

  @Test
  fun `value calls initializer again after invalidate`() {
    val initialValue: Int = lazy.value

    lazy.invalidate()

    val expectedValue = initialValue + 1
    assertThat(lazy.value).isEqualTo(expectedValue)
    assertThat(lazy.value).isEqualTo(expectedValue)
  }
}

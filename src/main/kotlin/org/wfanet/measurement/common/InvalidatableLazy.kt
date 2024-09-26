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

/** [Lazy] which can be invalidated. */
class InvalidatableLazy<T> internal constructor(private val initializer: () -> T) : Lazy<T> {
  @Volatile private var holder = Holder()

  /** Invalidates any existing instance, requiring it to be re-initialized on next access. */
  @Synchronized
  fun invalidate() {
    holder = Holder()
  }

  override val value: T
    get() = holder.delegate.value

  override fun isInitialized(): Boolean = holder.delegate.isInitialized()

  private inner class Holder {
    val delegate = lazy(initializer)
  }
}

/** Creates a new instance of an [InvalidatableLazy] using the [initializer] function. */
fun <T> invalidatableLazy(initializer: () -> T) = InvalidatableLazy(initializer)

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

/**
 * Represents a value with lazy initialization.
 *
 * Unlike [Lazy], this can be invalidated.
 *
 * To create an instance of [InvalidatableLazy], use the [invalidatableLazy] function.
 */
class InvalidatableLazy<T> internal constructor(private val initializer: () -> T) {
  @Volatile private var holder = Holder()

  /**
   * Invalidates any existing instance, requiring it to be re-initialized on next access.
   *
   * This causes [isInitialized] to return `false`.
   */
  @Synchronized
  fun invalidate() {
    holder = Holder()
  }

  /**
   * Gets the lazily initialized value of the current [InvalidatableLazy] instance.
   *
   * Once the value is initialized it will not change unless [invalidate] is called.
   */
  val value: T
    get() = holder.delegate.value

  /**
   * Returns whether the value has been initialized since creation of this [InvalidatableLazy] or
   * the last call to [invalidate], whichever comes latest.
   */
  fun isInitialized(): Boolean = holder.delegate.isInitialized()

  private inner class Holder {
    val delegate = lazy(initializer)
  }
}

/** Creates a new instance of an [InvalidatableLazy] using the [initializer] function. */
fun <T> invalidatableLazy(initializer: () -> T) = InvalidatableLazy(initializer)

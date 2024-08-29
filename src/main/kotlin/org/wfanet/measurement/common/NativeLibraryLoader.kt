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

interface NativeLibraryLoader {
  /** Loads a native library. */
  fun loadLibrary()

  companion object {
    /**
     * Returns the [NativeLibraryLoader] for the specified object declaration name, or `null` if not
     * found.
     */
    fun getLoaderByName(
      qualifiedName: String,
      module: Module = this::class.java.module,
    ): NativeLibraryLoader? {
      return Class.forName(module, qualifiedName)?.kotlin?.objectInstance as NativeLibraryLoader?
    }
  }
}

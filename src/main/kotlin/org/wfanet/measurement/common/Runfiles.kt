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

package org.wfanet.measurement.common

import com.google.devtools.build.runfiles.Runfiles
import java.nio.file.Path
import java.nio.file.Paths

private val runfiles: Runfiles by lazy { Runfiles.create() }

/**
 * Returns the runtime [Path] for the given runfiles-root-relative [Path], or null if it cannot be
 * found.
 *
 * Note that this may return a non-null value even if the path doesn't exist.
 *
 * @param runfilesRelativePath path relative to the Bazel runfiles root
 */
fun getRuntimePath(runfilesRelativePath: Path): Path? {
  return runfiles.rlocation(runfilesRelativePath.normalize().toString())?.let { Paths.get(it) }
}

/**
 * Loads a native library from the Bazel runfiles directory.
 *
 * @param name platform-independent library name
 * @param directoryPath normalized path of the directory the library is in, relative to the Bazel
 *   runfiles root
 * @deprecated This is not portable. Runfiles should only be used in test code where we can be sure
 *   of a Bazel context. Note that this is not needed to work around
 *   https://github.com/bazelbuild/rules_kotlin/issues/1088.
 */
@Deprecated(
  "Use System.loadLibrary, ensuring that the java.library.path property includes the path to the " +
    "native library"
)
fun loadLibrary(name: String, directoryPath: Path) {
  val relativePath = directoryPath.resolve(System.mapLibraryName(name))
  val runtimePath = requireNotNull(getRuntimePath(relativePath))

  System.load(runtimePath.toAbsolutePath().toString())
}

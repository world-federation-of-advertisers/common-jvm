/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.db.liquibase

import java.lang.UnsupportedOperationException
import java.nio.file.FileSystem
import java.nio.file.Files
import java.util.SortedSet
import liquibase.resource.AbstractResourceAccessor
import liquibase.resource.InputStreamList

/**
 * [liquibase.resource.ResourceAccessor] which accesses resources from the given [fileSystem].
 *
 * Note that this is not the same as [liquibase.resource.FileSystemResourceAccessor], which only
 * accesses files from [java.io.DefaultFileSystem] or from provided archives.
 */
internal class FileSystemResourceAccessor(private val fileSystem: FileSystem) :
  AbstractResourceAccessor() {
  /**
   * @see [AbstractResourceAccessor.openStreams]
   *
   * This implementation does not handle archives. Paths are expected to be valid for [fileSystem].
   */
  override fun openStreams(relativeTo: String?, streamPath: String?): InputStreamList {
    requireNotNull(streamPath)

    val path =
      if (relativeTo == null) {
        fileSystem.getPath(streamPath)
      } else {
        fileSystem.getPath(relativeTo).resolve(streamPath)
      }

    return InputStreamList(path.toUri(), Files.newInputStream(path))
  }

  override fun list(
    relativeTo: String?,
    path: String?,
    recursive: Boolean,
    includeFiles: Boolean,
    includeDirectories: Boolean,
  ): SortedSet<String> {
    throw UnsupportedOperationException()
  }

  override fun describeLocations(): SortedSet<String> {
    throw UnsupportedOperationException()
  }
}

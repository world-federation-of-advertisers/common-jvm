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

package org.wfanet.measurement.common

import java.net.URI
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.Paths

private val ZIP_FS_PROPERTIES: Map<String, Any> = mapOf("create" to "true")

/**
 * Returns a [Path] to the resource with the specified [name] within a JAR.
 *
 * This initializes the [java.io.FileSystem] for the
 * [zip file system provider](https://docs.oracle.com/javase/7/docs/technotes/guides/io/fsp/zipfilesystemprovider.html)
 * .
 */
fun ClassLoader.getJarResourcePath(name: String): Path? {
  val resourceUri: URI = getResource(name)?.toURI() ?: return null

  // Initialize zip FileSystem.
  FileSystems.newFileSystem(resourceUri, ZIP_FS_PROPERTIES)

  return Paths.get(resourceUri)
}

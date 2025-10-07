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

package org.wfanet.measurement.storage.filesystem

import com.google.protobuf.ByteString
import java.io.File
import java.nio.channels.Channels
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.relativeTo
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.storage.StorageClient

private const val READ_BUFFER_SIZE = 1024 * 4 // 4 KiB

/** [StorageClient] implementation that stores blobs as files under [directory]. */
class FileSystemStorageClient(
  private val directory: File,
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient {
  init {
    require(directory.isDirectory) { "$directory is not a directory" }
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val file: File = resolvePath(blobKey)
    println("-------------------- writing blob: ${file.path}")
    withContext(coroutineContext + CoroutineName("writeBlob")) {
      file.parentFile.mkdirs()
      file
        .outputStream()
        .buffered()
        .let { Channels.newChannel(it) }
        .use { byteChannel ->
          content.collect { bytes ->
            for (buffer in bytes.asReadOnlyByteBufferList()) {
              byteChannel.write(buffer)
            }
          }
        }
    }

    return Blob(file, blobKey)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val file: File = resolvePath(blobKey)
    println("-------------------- reading blob: ${file.path}")
    return withContext(coroutineContext + CoroutineName(::getBlob.name)) {
      if (file.exists()) Blob(file, blobKey) else null
    }
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    // Optimization to traverse fewer files if possible
    val pathStart: Path =
      if (prefix.isNullOrEmpty()) {
        directory.toPath()
      } else {
        if (Path.of(prefix).isAbsolute) {
          return emptyFlow()
        }

        val prefixFile = File(directory, prefix)
        if (!prefixFile.exists()) {
          directory.toPath()
        } else if (prefixFile.isFile) {
          return flowOf(Blob(prefixFile, prefix))
        } else {
          prefixFile.toPath()
        }
      }

    return channelFlow<StorageClient.Blob> {
        Files.walkFileTree(
          pathStart,
          object : SimpleFileVisitor<Path>() {
            override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
              if (attrs.isRegularFile) {
                val relativePath = file.relativeTo(directory.toPath())
                val blobKey = relativePath.toBlobKey()
                if (prefix.isNullOrEmpty() || blobKey.startsWith(prefix)) {
                  trySendBlocking(Blob(file.toFile(), blobKey))
                }
              }
              return FileVisitResult.CONTINUE
            }
          },
        )
      }
      .flowOn(coroutineContext)
  }

  private fun resolvePath(blobKey: String): File {
    val relativePath =
      if (File.separatorChar == '/') {
        blobKey
      } else {
        blobKey.replace('/', File.separatorChar)
      }
    return directory.resolve(relativePath)
  }

  private fun Path.toBlobKey(): String {
    return if (File.separatorChar == '/') {
      this.toString()
    } else {
      this.toString().replace(File.separatorChar, '/')
    }
  }

  private inner class Blob(private val file: File, override val blobKey: String) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@FileSystemStorageClient

    override val size: Long
      get() = file.length()

    override fun read(): Flow<ByteString> {
      return file
        .inputStream()
        .asFlow(READ_BUFFER_SIZE, coroutineContext + CoroutineName(::read.name))
    }

    override suspend fun delete() {
      file.delete()
    }
  }
}

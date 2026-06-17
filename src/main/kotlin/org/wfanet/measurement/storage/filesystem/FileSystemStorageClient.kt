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
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.time.Instant
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
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

private const val READ_BUFFER_SIZE = 1024 * 4 // 4 KiB

/**
 * [StorageClient] implementation that stores blobs as files under [directory].
 *
 * Implements [ConditionalOperationStorageClient] using the file's last-modified time as the
 * generation. Single-process semantics only: check-then-act on existing files is not atomic vs. a
 * concurrent writer for the same key. Safe for single-coroutine-per-key test usage.
 */
class FileSystemStorageClient(
  private val directory: File,
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient, ConditionalOperationStorageClient {
  init {
    require(directory.isDirectory) { "$directory is not a directory" }
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val file: File = resolvePath(blobKey)
    withContext(coroutineContext + CoroutineName("writeBlob")) {
      file.parentFile.mkdirs()
      val previousLastModified = if (file.exists()) file.lastModified() else 0L
      writeContentTo(file, content)
      // Ensure `lastModified` strictly advances. Two writes can land at the same millisecond on
      // fast hardware (typical filesystem resolution is 1ms or coarser), which would make this
      // file's generation indistinguishable from the prior one — breaking writeBlobIfUnchanged
      // for the very next caller.
      if (file.lastModified() <= previousLastModified) {
        file.setLastModified(previousLastModified + 1)
      }
    }

    return Blob(file, blobKey)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val file: File = resolvePath(blobKey)
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

  override suspend fun listBlobKeysAndPrefixes(prefix: String): Flow<String> {

    val prefixDir =
      if (prefix.isEmpty()) {
        directory
      } else {
        File(directory, prefix.trimEnd('/'))
      }

    if (!prefixDir.isDirectory) {
      return emptyFlow()
    }

    return channelFlow<String> {
        withContext(coroutineContext) {
          prefixDir.listFiles()?.forEach { entry ->
            val relativePath = entry.toPath().relativeTo(directory.toPath()).toBlobKey()
            if (entry.isDirectory) {
              trySendBlocking("$relativePath/")
            } else {
              trySendBlocking(relativePath)
            }
          }
        }
      }
      .flowOn(coroutineContext)
  }

  /**
   * Conditional write that succeeds only if the file's current `lastModified` equals
   * [expectedGeneration] (`0` for write-if-absent). Atomic for the absent case via
   * `Files.createFile`; for the present case it is a non-atomic check-then-act and the file is then
   * replaced via an atomic move.
   */
  override suspend fun writeBlobIfGeneration(
    blobKey: String,
    expectedGeneration: Long,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(expectedGeneration >= 0) { "expectedGeneration must be >= 0, got $expectedGeneration" }
    val file: File = resolvePath(blobKey)
    return withContext(coroutineContext + CoroutineName("writeBlobIfGeneration")) {
      file.parentFile.mkdirs()
      if (expectedGeneration == 0L) {
        // Write-if-absent: atomic create via NIO; throws FileAlreadyExistsException if present.
        try {
          Files.createFile(file.toPath())
        } catch (e: FileAlreadyExistsException) {
          throw BlobChangedException("Blob already exists: $blobKey", e)
        }
        writeContentTo(file, content)
      } else {
        if (!file.exists()) {
          throw BlobChangedException("Blob does not exist: $blobKey")
        }
        if (file.lastModified() != expectedGeneration) {
          throw BlobChangedException(
            "Blob $blobKey is at generation ${file.lastModified()}, not $expectedGeneration"
          )
        }
        // Write to a temp sibling and atomically move into place, so partially-written content
        // is never visible.
        val temp = File.createTempFile(file.name, ".tmp", file.parentFile)
        writeContentTo(temp, content)
        Files.move(
          temp.toPath(),
          file.toPath(),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING,
        )
      }
      Blob(file, blobKey)
    }
  }

  override suspend fun writeBlobIfUnchanged(
    blob: StorageClient.Blob,
    content: Flow<ByteString>,
  ): StorageClient.Blob {
    require(blob is Blob) { "Incompatible blob type" }
    require(blob.storageClient === this) { "Blob does not belong to this storage client" }
    return writeBlobIfGeneration(blob.blobKey, blob.generation, content)
  }

  /** Writes [content] to [file] using a buffered channel. Caller manages file creation. */
  private suspend fun writeContentTo(file: File, content: Flow<ByteString>) {
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

  internal inner class Blob(private val file: File, override val blobKey: String) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@FileSystemStorageClient

    /**
     * Filesystem generation = file's `lastModified` snapshotted at this [Blob]'s construction. Must
     * NOT track mutations to the underlying file — `writeBlobIfUnchanged` relies on a stale
     * generation to detect that the file has changed since the [Blob] was last observed.
     *
     * Monotonic by construction: [writeBlob] forces `lastModified` to strictly advance across
     * writes to defeat millisecond-resolution aliasing, so this value may exceed the file's actual
     * wall-clock modification time by a few milliseconds.
     */
    internal val generation: Long = file.lastModified()

    override val size: Long
      get() = file.length()

    override val createTime: Instant
      get() =
        Files.readAttributes(file.toPath(), BasicFileAttributes::class.java)
          .creationTime()
          .toInstant()

    override val updateTime: Instant
      get() = Instant.ofEpochMilli(file.lastModified())

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

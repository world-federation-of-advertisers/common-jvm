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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.storage.StorageClient

private const val READ_BUFFER_SIZE = 1024 * 4 // 4 KiB

/** [StorageClient] implementation that stores blobs as files under [directory]. */
class FileSystemStorageClient(private val directory: File) : StorageClient {
  init {
    require(directory.isDirectory) { "$directory is not a directory" }
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val file: File = resolvePath(blobKey)
    withContext(Dispatchers.IO) {
      file.parentFile.mkdirs()
      file.outputStream().channel.use { byteChannel ->
        content.collect { bytes ->
          for (buffer in bytes.asReadOnlyByteBufferList()) {
            @Suppress("BlockingMethodInNonBlockingContext") // Flow context preservation.
            byteChannel.write(buffer)
          }
        }
      }
    }

    return Blob(file)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val file: File = resolvePath(blobKey)
    return withContext(Dispatchers.IO) { if (file.exists()) Blob(file) else null }
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

  private inner class Blob(private val file: File) : StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@FileSystemStorageClient

    override val size: Long
      get() = file.length()

    override fun read(): Flow<ByteString> = file.inputStream().asFlow(READ_BUFFER_SIZE)

    override suspend fun delete() {
      file.delete()
    }
  }
}

private fun String.base64UrlEncode(): String {
  return toByteArray().base64UrlEncode()
}

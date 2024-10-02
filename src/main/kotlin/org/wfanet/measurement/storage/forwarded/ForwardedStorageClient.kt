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

package org.wfanet.measurement.storage.forwarded

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.internal.testing.WriteBlobRequest
import org.wfanet.measurement.internal.testing.deleteBlobRequest
import org.wfanet.measurement.internal.testing.getBlobMetadataRequest
import org.wfanet.measurement.internal.testing.readBlobRequest
import org.wfanet.measurement.storage.StorageClient

/**
 * Size of byte buffer used for blob writes.
 *
 * See https://github.com/grpc/grpc.github.io/issues/371.
 */
private const val WRITE_BUFFER_SIZE = 1024 * 32 // 32 KiB

/** [StorageClient] for ForwardedStorage service. */
class ForwardedStorageClient(private val storageStub: ForwardedStorageCoroutineStub) :
  StorageClient {

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    // gRPC has a hard limit on message size, so we always rebuffer to make sure we're under that.
    val rebufferedContent = content.asBufferedFlow(WRITE_BUFFER_SIZE)

    val requests =
      rebufferedContent
        .map { WriteBlobRequest.newBuilder().apply { bodyChunkBuilder.content = it }.build() }
        .onStart {
          emit(WriteBlobRequest.newBuilder().apply { headerBuilder.blobKey = blobKey }.build())
        }
    val metadata =
      try {
        storageStub.writeBlob(requests)
      } catch (e: StatusException) {
        throw Exception("Error writing blob $blobKey", e)
      }

    return Blob(blobKey, metadata.size)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    // Check if the blob exists
    val blobSize =
      try {
        storageStub.getBlobMetadata(getBlobMetadataRequest { this.blobKey = blobKey }).size
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.NOT_FOUND) {
          return null
        } else {
          throw Exception("Error getting blob $blobKey", e)
        }
      }

    return Blob(blobKey, blobSize)
  }

  private inner class Blob(private val blobKey: String, override val size: Long) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@ForwardedStorageClient

    override fun read(): Flow<ByteString> {
      return storageStub
        .readBlob(readBlobRequest { blobKey = this@Blob.blobKey })
        .catch { cause ->
          if (cause is StatusException) throw Exception("Error reading blob $blobKey")
        }
        .map { it.chunk }
    }

    override suspend fun delete() {
      try {
        storageStub.deleteBlob(deleteBlobRequest { blobKey = this@Blob.blobKey })
      } catch (e: Exception) {
        throw Exception("Error deleting blob $blobKey", e)
      }
    }
  }
}

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
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.WriteBlobRequest
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
    val metadata = storageStub.writeBlob(requests)

    return Blob(blobKey, metadata.size)
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    // Check if the blob exists
    val blobSize =
      try {
        runBlocking {
          storageStub.getBlobMetadata(
              GetBlobMetadataRequest.newBuilder().setBlobKey(blobKey).build()
            )
            .size
        }
      } catch (e: StatusException) {
        if (e.status.code == Status.NOT_FOUND.code) {
          return null
        } else {
          throw e
        }
      }

    return Blob(blobKey, blobSize)
  }

  private inner class Blob(private val blobKey: String, override val size: Long) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@ForwardedStorageClient

    override fun read(): Flow<ByteString> {
      return storageStub.readBlob(readBlobRequest { blobKey = this@Blob.blobKey }).map { it.chunk }
    }

    override fun delete() =
      runBlocking<Unit> {
        storageStub.deleteBlob(DeleteBlobRequest.newBuilder().setBlobKey(blobKey).build())
      }
  }
}

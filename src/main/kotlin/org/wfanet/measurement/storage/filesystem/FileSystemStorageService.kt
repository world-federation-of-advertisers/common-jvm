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

import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.consumeFirstOr
import org.wfanet.measurement.internal.testing.BlobMetadata
import org.wfanet.measurement.internal.testing.DeleteBlobRequest
import org.wfanet.measurement.internal.testing.DeleteBlobResponse
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineImplBase as ForwardedStorageCoroutineService
import org.wfanet.measurement.internal.testing.GetBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ListBlobMetadataRequest
import org.wfanet.measurement.internal.testing.ListBlobMetadataResponse
import org.wfanet.measurement.internal.testing.ReadBlobRequest
import org.wfanet.measurement.internal.testing.ReadBlobResponse
import org.wfanet.measurement.internal.testing.WriteBlobRequest
import org.wfanet.measurement.internal.testing.blobMetadata
import org.wfanet.measurement.internal.testing.listBlobMetadataResponse
import org.wfanet.measurement.internal.testing.readBlobResponse

/** [ForwardedStorageCoroutineService] implementation that uses [FileSystemStorageClient]. */
class FileSystemStorageService(
  directory: File,
  coroutineContext: @BlockingExecutor CoroutineContext,
) : ForwardedStorageCoroutineService() {
  val storageClient: FileSystemStorageClient = FileSystemStorageClient(directory, coroutineContext)

  override suspend fun writeBlob(requests: Flow<WriteBlobRequest>): BlobMetadata {
    val blob =
      requests
        .consumeFirstOr { WriteBlobRequest.getDefaultInstance() }
        .use { consumed ->
          val headerRequest = consumed.item
          val blobKey = headerRequest.header.blobKey
          if (blobKey.isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("Missing blob key").asRuntimeException()
          }

          val content = consumed.remaining.map { it.bodyChunk.content }
          storageClient.writeBlob(blobKey, content)
        }

    return BlobMetadata.newBuilder().setSize(blob.size).build()
  }

  override suspend fun getBlobMetadata(request: GetBlobMetadataRequest): BlobMetadata {
    return blobMetadata {
      size = getBlob(request.blobKey).size
      blobKey = request.blobKey
    }
  }

  override fun readBlob(request: ReadBlobRequest): Flow<ReadBlobResponse> = flow {
    emitAll(getBlob(request.blobKey).read().map { readBlobResponse { chunk = it } })
  }

  override suspend fun deleteBlob(request: DeleteBlobRequest): DeleteBlobResponse {
    getBlob(request.blobKey).delete()
    return DeleteBlobResponse.getDefaultInstance()
  }

  override suspend fun listBlobMetadata(
    request: ListBlobMetadataRequest
  ): ListBlobMetadataResponse {
    return listBlobMetadataResponse {
      blobMetadata +=
        storageClient.listBlobs(prefix = request.prefix).map {
          blobMetadata {
            blobKey = it.blobKey
            size = it.size
          }
        }
    }
  }

  private suspend fun getBlob(blobKey: String) =
    storageClient.getBlob(blobKey)
      ?: throw StatusException(Status.NOT_FOUND.withDescription("Blob not found with key $blobKey"))
}

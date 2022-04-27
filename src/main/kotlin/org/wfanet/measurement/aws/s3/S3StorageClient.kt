// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.aws.s3

import com.google.protobuf.ByteString
import java.security.MessageDigest
import java.util.Base64
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.storage.StorageClient
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.UploadPartRequest

/** S3 requires each part of a multipart upload (except the last) is at least 5 MB. */
private const val WRITE_BUFFER_SIZE = BYTES_PER_MIB * 5
private const val READ_BUFFER_SIZE = WRITE_BUFFER_SIZE

/** Amazon Web Services (AWS) S3 implementation of [StorageClient] for a single bucket. */
class S3StorageClient(
  /**
   * Amazon S3 client.
   *
   * TODO(@SanjayVas): Use [software.amazon.awssdk.services.s3.S3AsyncClient] instead
   */
  private val s3: S3Client,
  private val bucketName: String
) : StorageClient {
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val uploadId = createMultipartUpload(blobKey)

    try {
      val completedParts = uploadParts(blobKey, content, uploadId)
      completeUpload(blobKey, uploadId, completedParts)
    } catch (e: Exception) {
      cancelUpload(blobKey, uploadId)
      throw e
    }

    return checkNotNull(getBlob(blobKey)) { "Blob key $blobKey was uploaded but no longer exists" }
  }

  private suspend fun createMultipartUpload(blobKey: String): String {
    val multipartUploadRequest =
      CreateMultipartUploadRequest.builder().bucket(bucketName).key(blobKey).build()
    val multipartUploadResponse =
      withContext(Dispatchers.IO) { s3.createMultipartUpload(multipartUploadRequest) }
    return multipartUploadResponse.uploadId()
  }

  private suspend fun uploadParts(
    blobKey: String,
    content: Flow<ByteString>,
    uploadId: String
  ): List<CompletedPart> {
    val builder = UploadPartRequest.builder().uploadId(uploadId).key(blobKey).bucket(bucketName)
    return content
      .asBufferedFlow(WRITE_BUFFER_SIZE)
      .withIndex()
      .map { (i, bytes) ->
        val digest = MessageDigest.getInstance("MD5")
        val md5 = String(Base64.getEncoder().encode(digest.digest(bytes.toByteArray())))

        val uploadPartRequest =
          builder.partNumber(i + 1).contentLength(bytes.size().toLong()).contentMD5(md5).build()

        val uploadPartResponse =
          s3.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(bytes.asReadOnlyByteBuffer()))

        CompletedPart.builder().partNumber(i + 1).eTag(uploadPartResponse.eTag()).build()
      }
      .toList()
  }

  private suspend fun completeUpload(
    blobKey: String,
    uploadId: String,
    completedParts: Collection<CompletedPart>
  ): CompleteMultipartUploadResponse {
    return withContext(Dispatchers.IO) {
      s3.completeMultipartUpload { builder ->
        builder.uploadId(uploadId)
        builder.bucket(bucketName)
        builder.key(blobKey)

        builder.multipartUpload { it.parts(completedParts) }
      }
    }
  }

  private suspend fun cancelUpload(blobKey: String, uploadId: String) =
    withContext(Dispatchers.IO) {
      s3.abortMultipartUpload {
        it.uploadId(uploadId)
        it.bucket(bucketName)
        it.key(blobKey)
      }
    }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val head = head(blobKey) ?: return null
    return Blob(blobKey, head)
  }

  private suspend fun head(blobKey: String): HeadObjectResponse? {
    return try {
      withContext(Dispatchers.IO) {
        s3.headObject {
          it.bucket(bucketName)
          it.key(blobKey)
        }
      }
    } catch (notFound: NoSuchKeyException) {
      null
    }
  }

  inner class Blob internal constructor(private val blobKey: String, head: HeadObjectResponse) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@S3StorageClient

    override val size: Long = head.contentLength()

    override fun read(): Flow<ByteString> {
      val flow = flow {
        val input =
          s3.getObject {
            it.bucket(bucketName)
            it.key(blobKey)
          }

        emitAll(input.asFlow(READ_BUFFER_SIZE))
      }
      return flow.flowOn(Dispatchers.IO)
    }

    override suspend fun delete(): Unit =
      withContext(Dispatchers.IO) {
        s3.deleteObject {
          it.bucket(bucketName)
          it.key(blobKey)
        }
      }
  }
}

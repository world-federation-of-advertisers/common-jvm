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
import com.google.protobuf.kotlin.toByteString
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEmpty
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.update
import org.wfanet.measurement.storage.StorageClient
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.async.ResponsePublisher
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.services.s3.model.UploadPartResponse

/** S3 requires each part of a multipart upload (except the last) is at least 5 MB. */
private const val WRITE_BUFFER_SIZE = BYTES_PER_MIB * 5

/** Amazon Web Services (AWS) S3 implementation of [StorageClient] for a single bucket. */
class S3StorageClient(private val s3: S3AsyncClient, private val bucketName: String) :
  StorageClient {
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val uploadId: String = createMultipartUpload(blobKey).uploadId()

    try {
      val completedParts: Flow<CompletedPart> = uploadParts(blobKey, content, uploadId)
      completeUpload(blobKey, uploadId, completedParts)
    } catch (e: Exception) {
      cancelUpload(blobKey, uploadId)
      throw e
    }

    return checkNotNull(getBlob(blobKey)) { "Blob key $blobKey was uploaded but no longer exists" }
  }

  private suspend fun createMultipartUpload(blobKey: String): CreateMultipartUploadResponse {
    val request = CreateMultipartUploadRequest.builder().bucket(bucketName).key(blobKey).build()
    return s3.createMultipartUpload(request).await()
  }

  private fun uploadParts(
    blobKey: String,
    content: Flow<ByteString>,
    uploadId: String,
  ): Flow<CompletedPart> {
    val digest = MessageDigest.getInstance("MD5")
    val requestBuilder =
      UploadPartRequest.builder().uploadId(uploadId).key(blobKey).bucket(bucketName)
    return content
      .asBufferedFlow(WRITE_BUFFER_SIZE)
      .withIndex()
      .onEmpty { emit(IndexedValue(0, ByteString.EMPTY)) }
      .map { (i, bytes) ->
        val partNumber = i + 1
        digest.update(bytes)
        val md5 = String(Base64.getEncoder().encode(digest.digest()))

        val request: UploadPartRequest =
          requestBuilder
            .partNumber(partNumber)
            .contentLength(bytes.size().toLong())
            .contentMD5(md5)
            .build()

        val response: UploadPartResponse =
          s3
            .uploadPart(request, AsyncRequestBody.fromByteBuffer(bytes.asReadOnlyByteBuffer()))
            .await()

        CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
      }
  }

  private suspend fun completeUpload(
    blobKey: String,
    uploadId: String,
    completedParts: Flow<CompletedPart>,
  ): CompleteMultipartUploadResponse {
    val completedPartsList = completedParts.toList()
    return s3
      .completeMultipartUpload { builder ->
        builder.uploadId(uploadId)
        builder.bucket(bucketName)
        builder.key(blobKey)
        builder.multipartUpload { it.parts(completedPartsList) }
      }
      .await()
  }

  private suspend fun cancelUpload(
    blobKey: String,
    uploadId: String,
  ): AbortMultipartUploadResponse {
    return s3
      .abortMultipartUpload {
        it.uploadId(uploadId)
        it.bucket(bucketName)
        it.key(blobKey)
      }
      .await()
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val head = head(blobKey) ?: return null
    return Blob(blobKey, head)
  }

  private suspend fun head(blobKey: String): HeadObjectResponse? {
    return try {
      s3
        .headObject {
          it.bucket(bucketName)
          it.key(blobKey)
        }
        .await()
    } catch (notFound: NoSuchKeyException) {
      null
    }
  }

  override suspend fun listBlobNames(prefix: String, delimiter: String): List<String> {
    if (prefix.isEmpty()) {
      throw IllegalArgumentException("Prefix must not be empty")
    }

    return try {
      var truncated = true
      var continuationToken: String? = null
      buildList {
        while (truncated) {
          val listObjectsV2Response: ListObjectsV2Response =
            s3
              .listObjectsV2 {
                it.bucket(bucketName)
                it.prefix(prefix)
                if (delimiter.isNotEmpty()) {
                  it.delimiter(delimiter)
                }
                if (continuationToken != null) {
                  it.continuationToken(continuationToken)
                }
              }
              .await()

          truncated = listObjectsV2Response.isTruncated
          continuationToken = listObjectsV2Response.nextContinuationToken()

          addAll(listObjectsV2Response.contents().map { it.key() })

          addAll(listObjectsV2Response.commonPrefixes().map { it.prefix() })
        }
      }
    } catch (e: CompletionException) {
      throw e.cause!!
    }
  }

  inner class Blob internal constructor(private val blobKey: String, head: HeadObjectResponse) :
    StorageClient.Blob {
    override val storageClient: StorageClient
      get() = this@S3StorageClient

    override val size: Long = head.contentLength()

    override fun read(): Flow<ByteString> {
      val responseFuture: CompletableFuture<ResponsePublisher<GetObjectResponse>> =
        s3.getObject(
          {
            it.bucket(bucketName)
            it.key(blobKey)
          },
          AsyncResponseTransformer.toPublisher(),
        )

      return flow { emitAll(responseFuture.await().asFlow().map { it.toByteString() }) }
    }

    override suspend fun delete() {
      s3
        .deleteObject {
          it.bucket(bucketName)
          it.key(blobKey)
        }
        .await()
    }
  }

  companion object {
    /** Constructs a [S3StorageClient] from command-line flags. */
    fun fromFlags(s3Flags: S3Flags) =
      S3StorageClient(S3AsyncClient.builder().region(s3Flags.s3Region).build(), s3Flags.s3Bucket)
  }
}

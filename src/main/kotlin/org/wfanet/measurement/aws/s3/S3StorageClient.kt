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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.withIndex
import org.wfanet.measurement.common.BYTES_PER_MIB
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.asFlow
import org.wfanet.measurement.storage.StorageClient
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.UploadPartRequest

/** S3 requires each part of a multipart upload (except the last) is at least 5 MB. */
private const val BYTE_BUFFER_SIZE = BYTES_PER_MIB * 5

/** Amazon Web Services (AWS) S3 implementation of [StorageClient] for a single bucket. */
class S3StorageClient(private val s3: S3Client, private val bucketName: String) : StorageClient {
  override val defaultBufferSizeBytes: Int = BYTE_BUFFER_SIZE

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
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

  private fun createMultipartUpload(blobKey: String): String {
    val multipartUploadRequest =
      CreateMultipartUploadRequest.builder().bucket(bucketName).key(blobKey).build()
    val multipartUploadResponse = s3.createMultipartUpload(multipartUploadRequest)
    return multipartUploadResponse.uploadId()
  }

  private suspend fun uploadParts(
    blobKey: String,
    content: Flow<ByteString>,
    uploadId: String
  ): List<CompletedPart> {
    val digest = MessageDigest.getInstance("MD5")
    val builder = UploadPartRequest.builder().uploadId(uploadId).key(blobKey).bucket(bucketName)
    return content
      .asBufferedFlow(BYTE_BUFFER_SIZE)
      .withIndex()
      .map { (i, bytes) ->
        bytes.asReadOnlyByteBufferList().forEach(digest::update)
        val md5 = HexString(digest.digest().toByteString())

        val uploadPartRequest =
          builder
            .partNumber(i + 1)
            .contentLength(bytes.size().toLong())
            .contentMD5(md5.value)
            .build()

        val uploadPartResponse =
          s3.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(bytes.asReadOnlyByteBuffer()))

        CompletedPart.builder().partNumber(i + 1).eTag(uploadPartResponse.eTag()).build()
      }
      .toList()
  }

  private fun completeUpload(
    blobKey: String,
    uploadId: String,
    completedParts: Iterable<CompletedPart>
  ) {
    s3.completeMultipartUpload { builder ->
      builder.uploadId(uploadId)
      builder.bucket(bucketName)
      builder.key(blobKey)

      builder.multipartUpload { it.parts(completedParts.toList()) }
    }
  }

  private fun cancelUpload(blobKey: String, uploadId: String) {
    s3.abortMultipartUpload {
      it.uploadId(uploadId)
      it.bucket(bucketName)
      it.key(blobKey)
    }
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    val head = head(blobKey) ?: return null
    return Blob(blobKey, head)
  }

  private fun head(blobKey: String): HeadObjectResponse? {
    return try {
      s3.headObject {
        it.bucket(bucketName)
        it.key(blobKey)
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

    override fun read(bufferSizeBytes: Int): Flow<ByteString> {
      val input =
        s3.getObject {
          it.bucket(bucketName)
          it.key(blobKey)
        }

      return input.asFlow(bufferSizeBytes)
    }

    override fun delete() {
      s3.deleteObject {
        it.bucket(bucketName)
        it.key(blobKey)
      }
    }
  }
}

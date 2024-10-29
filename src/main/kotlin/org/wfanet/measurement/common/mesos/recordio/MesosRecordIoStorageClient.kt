// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.mesos.recordio

import com.google.protobuf.ByteString
import java.util.logging.Logger
import org.wfanet.measurement.storage.StorageClient
import java.io.ByteArrayOutputStream
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * A wrapper class for the [StorageClient] interface that handles Apache Mesos RecordIO formatted files
 * for blob/object storage operations.
 *
 * This class supports row-based reading and writing, enabling the processing of individual rows
 * at the client's pace. The implementation focuses on handling record-level operations for
 * RecordIO files.
 *
 * @param storageClient underlying client for accessing blob/object storage
 */
class MesosRecordIoStorageClient(private val storageClient: StorageClient) : StorageClient {

  /**
  * Writes RecordIO rows to storage using the RecordIO format.
  *
  * This function takes a flow of RecordIO rows (represented as a Flow<ByteString>), formats each row
  * by prepending the record size and a newline character (`\n`), and writes the formatted content
  * to storage.
  *
  * The function handles rows emitted at any pace, meaning it can process rows that are emitted asynchronously
  * with delays in between.
  *
  * @param blobKey The key (or name) of the blob where the content will be stored.
  * @param content A Flow<ByteString> representing the source of RecordIO rows that will be stored.
  *
  * @return A Blob object representing the RecordIO data that was written to storage.
  */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val processedContent = flow {
      val outputStream = ByteArrayOutputStream()

      content.collect { byteString ->
        val rawBytes = byteString.toByteArray()
        val recordSize = rawBytes.size.toString()
        val fullRecord = recordSize + "\n" + String(rawBytes, Charsets.UTF_8)
        val fullRecordBytes = fullRecord.toByteArray(Charsets.UTF_8)
        outputStream.write(fullRecordBytes)
        emit(ByteString.copyFrom(outputStream.toByteArray()))
        outputStream.reset()
      }

      val remainingBytes = outputStream.toByteArray()
      if (remainingBytes.isNotEmpty()) {
        emit(ByteString.copyFrom(remainingBytes))
      }
    }

    val wrappedBlob: StorageClient.Blob = storageClient.writeBlob(blobKey, processedContent)
    logger.fine { "Wrote content to storage with blobKey: $blobKey" }
    return RecordioBlob(wrappedBlob, blobKey)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is read as RecordIO format when [RecordioBlob.read] is called
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { RecordioBlob(it, blobKey) }
  }

  /** A blob that will read the content in RecordIO format */
  private inner class RecordioBlob(private val blob: StorageClient.Blob, private val blobKey: String) :
    StorageClient.Blob {
    override val storageClient = this@MesosRecordIoStorageClient.storageClient

    override val size: Long
      get() = blob.size

    /**
     * Reads data from storage in RecordIO format, streaming chunks of data and processing them
     * on-the-fly to extract individual records.
     *
     * The function reads each row from an Apache Mesos RecordIO file and emits them individually.
     * Each record in the RecordIO format begins with its size followed by a newline character,
     * then the actual record data.
     *
     * @return A Flow of ByteString, where each emission represents a complete record from the
     *         RecordIO formatted data.
     *
     * @throws java.io.IOException If there is an issue reading from the stream.
     */
    override fun read(): Flow<ByteString> = flow {
      val buffer = StringBuilder()
      var currentRecordSize = -1
      var recordBuffer = ByteArrayOutputStream()

      blob.read().collect { chunk ->
        var position = 0
        val chunkString = chunk.toByteArray().toString(Charsets.UTF_8)

        while (position < chunkString.length) {
          if (currentRecordSize == -1) {
            while (position < chunkString.length) {
              val char = chunkString[position++]
              if (char == '\n') {
                currentRecordSize = buffer.toString().toInt()
                buffer.clear()
                recordBuffer = ByteArrayOutputStream(currentRecordSize)
                break
              }
              buffer.append(char)
            }
          }
          if (currentRecordSize > 0) {
            val remainingBytes = chunkString.length - position
            val bytesToRead = minOf(remainingBytes, currentRecordSize - recordBuffer.size())

            if (bytesToRead > 0) {
              recordBuffer.write(
                chunkString.substring(position, position + bytesToRead)
                  .toByteArray(Charsets.UTF_8)
              )
              position += bytesToRead
            }
            if (recordBuffer.size() == currentRecordSize) {
              emit(ByteString.copyFrom(recordBuffer.toByteArray()))
              currentRecordSize = -1
              recordBuffer = ByteArrayOutputStream()
            }
          }
        }
      }
    }

    override suspend fun delete() = blob.delete()

  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}

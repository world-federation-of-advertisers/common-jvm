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

package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlin.ByteArray
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * A wrapper class for the [StorageClient] interface that handles Apache Mesos RecordIO formatted
 * files for blob/object storage operations.
 *
 * This class supports row-based reading and writing, enabling the processing of individual rows at
 * the client's pace. The implementation focuses on handling record-level operations for RecordIO
 * files.
 *
 * @param storageClient underlying client for accessing blob/object storage
 */
// TODO(@marcopremier): Refactor into MesosRecordIoStore<T : Message> to handle proto message
// serialization internally and make current StorageClient implementation private.
class MesosRecordIoStorageClient(private val storageClient: StorageClient) : StorageClient {

  /**
   * Writes RecordIO rows to storage using the RecordIO format.
   *
   * This function takes a flow of RecordIO rows (represented as a Flow<ByteString>), formats each
   * row by prepending the record size and a newline character (`\n`), and writes the formatted
   * content to storage.
   *
   * The function handles rows emitted at any pace, meaning it can process rows that are emitted
   * asynchronously with delays in between.
   *
   * Produces a flow where each emission represents a single record.
   *
   * @param blobKey The key (or name) of the blob where the content will be stored.
   * @param content A Flow<ByteString> representing the source of RecordIO rows that will be stored.
   * @return A Blob object representing the RecordIO data that was written to storage.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    var recordsWritten = 0
    val processedContent = flow {
      val outputStream: ByteString.Output = ByteString.newOutput()
      content.collect { byteString ->
        val byteArray = byteString.toByteArray()
        val recordSize: String = byteArray.size.toString()
        val fullRecordBytes: ByteArray =
          (recordSize + RECORD_DELIMITER).toByteArray(Charsets.UTF_8) + byteArray
        outputStream.write(fullRecordBytes)
        recordsWritten++
        emit(outputStream.toByteString())
        outputStream.reset()
      }
    }

    val wrappedBlob: StorageClient.Blob = storageClient.writeBlob(blobKey, processedContent)
    logger.fine { "Wrote $recordsWritten records to storage with blobKey: $blobKey" }
    return Blob(wrappedBlob, blobKey)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is read as RecordIO format when [Blob.read] is called
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { Blob(it, blobKey) }
  }

  /** A blob that will read the content in RecordIO format */
  private inner class Blob(private val blob: StorageClient.Blob, private val blobKey: String) :
    StorageClient.Blob {
    override val storageClient = this@MesosRecordIoStorageClient.storageClient

    override val size: Long
      get() = blob.size

    private fun ByteArray.indexOf(targetString: String, position: Int = 0): Int {
      val targetBytes = targetString.toByteArray()
      if (position < 0 || position >= this.size) {
        return -1 // Invalid position
      }
      for (i in position..this.size - targetBytes.size) {
        if (this.sliceArray(i until i + targetBytes.size).contentEquals(targetBytes)) {
          return i
        }
      }
      return -1 // Not found
    }

    /**
     * Reads data from storage in RecordIO format, streaming chunks of data and processing them
     * on-the-fly to extract individual records.
     *
     * The function reads each row from an Apache Mesos RecordIO file and emits them individually.
     * Each record in the RecordIO format begins with its size followed by a newline character, then
     * the actual record data.
     *
     * @return A Flow of ByteString, where each emission represents a complete record from the
     *   RecordIO formatted data.
     * @throws java.io.IOException If there is an issue reading from the stream.
     */
    override fun read(): Flow<ByteString> = flow {
      val recordSizeBuffer = StringBuilder()
      var currentRecordSize = -1
      var recordBuffer = ByteString.newOutput()

      blob.read().collect { chunk ->
        var position = 0
        val chunkByteArray = chunk.toByteArray()

        while (position < chunkByteArray.size) {
          if (currentRecordSize == -1) {

            val newlineIndex = chunkByteArray.indexOf(RECORD_DELIMITER, position)
            if (newlineIndex != -1) {
              require(newlineIndex != position)
              recordSizeBuffer.append(
                chunkByteArray.sliceArray(position until newlineIndex).toString(Charsets.UTF_8)
              )
              currentRecordSize = recordSizeBuffer.toString().toInt()
              recordSizeBuffer.clear()
              recordBuffer = ByteString.newOutput(currentRecordSize)
              position = newlineIndex + 1
            } else {
              recordSizeBuffer.append(chunkByteArray.toString(Charsets.UTF_8))
              break
            }
          }
          if (currentRecordSize > 0) {
            val remainingBytes = chunkByteArray.size - position
            val bytesToRead = minOf(remainingBytes, currentRecordSize - recordBuffer.size())

            if (bytesToRead > 0) {
              recordBuffer.write(chunkByteArray.sliceArray(position until position + bytesToRead))
              position += bytesToRead
            }
            if (recordBuffer.size() == currentRecordSize) {
              emit(recordBuffer.toByteString())
              currentRecordSize = -1
              recordBuffer.reset()
            }
          }
        }
      }
    }

    override suspend fun delete() = blob.delete()
  }

  companion object {
    private const val RECORD_DELIMITER = "\n"
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

// Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.runBlocking

/**
 * A wrapper class for the [StorageClient] interface that handles writing and reading data into
 * shards
 *
 * This class supports chunking data and writing it into separate shards automatically
 *
 * @param underlyingStorageClient underlying client for accessing blob/object storage
 */
class ShardedStorageClient(private val underlyingStorageClient: MesosRecordIoStorageClient) :
  StorageClient {
  /**
   * Writes sharded data to MesosRecordIoStorage after it has been split into chunks.
   *
   * This function takes a flow of ByteString, splits it into chunks, and stores it in different
   * shards. The sharded data will be stored at the location {blobKey}-x-of-N, where x is the shard
   * index and N is the total number of shards. The data stored at the location blobKey is metadata
   * that describes the number of shards in the format "{blobKey}-*-of-N", where N is the total
   * number of shards.
   *
   * @param blobKey Logical directory where the content will be stored.
   * @param content A Flow<ByteString> representing the source of data that will be stored.
   * @param chunkSize The size of the chunks that the data will be split into
   * @return A Blob object representing file that contains metadata on the sharded data the was
   *   written to storage. The file will be in the format "{blobKey}-*-of-N", where N represents the
   *   total number of shards the data was split into.
   */
  suspend fun writeBlob(
    blobKey: String,
    content: Flow<ByteString>,
    chunkSize: Int,
  ): StorageClient.Blob {
    var currentChunk = mutableListOf<ByteString>()
    var chunkIndex = 0

    // Shard current Flow<ByteString> into smaller Flow<ByteString> chunks of size chunkSize
    content.collect { byteString ->
      currentChunk.add(byteString)
      if (currentChunk.size == chunkSize) {
        chunkIndex++
        // Write all smaller Flow<ByteString> chunks to storage using the same blob key concatenated
        // with the indexed entry of each shard with the pattern blobKey/chunk-x
        underlyingStorageClient.writeBlob(
          getChunkBlobKey(blobKey, chunkIndex),
          currentChunk.asFlow()
        )
        currentChunk = mutableListOf()
      }
    }
    // Add any remaining elements as the last chunk
    if (currentChunk.isNotEmpty()) {
      chunkIndex++
      underlyingStorageClient.writeBlob(
        getChunkBlobKey(blobKey, chunkIndex),
        currentChunk.asFlow()
      )
    }
    // Write a string to storage that signifies the size of the sharded content and return Blob with
    // this metadata
    val shardData = listOf("$blobKey/chunk-*-of-${chunkIndex}".toByteStringUtf8())
    return underlyingStorageClient.writeBlob("$blobKey/$CHUNK_METADATA_SUFFIX", shardData.asFlow())
  }

  /**
   * Writes data to storage.
   *
   * This function will call writeBlob(blobKey: String, content: Flow<ByteString>, chunkSize: Int)
   * with the DEFAULT_CHUNK_SIZE
   *
   * @param blobKey The key (or name) of the blob where the content will be stored.
   * @param content A Flow<ByteString> representing the source of RecordIO rows that will be stored.
   * @return A Blob object representing file that was written to storage.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return writeBlob(blobKey, content, DEFAULT_CHUNK_SIZE)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is read as format when [Blob.read] is called.
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = underlyingStorageClient.getBlob("$blobKey/$CHUNK_METADATA_SUFFIX") ?: return null
    return Blob(blob, blobKey)
  }

  private inner class Blob(
    private val metadataBlob: StorageClient.Blob,
    private val blobKey: String,
    override val storageClient: MesosRecordIoStorageClient =
      this@ShardedStorageClient.underlyingStorageClient,
  ) : StorageClient.Blob {
    override val size: Long
      get() = metadataBlob.size

    /**
     * Reads data from MesosRecordIoStorage.
     *
     * This function reads sharded data from storage by 1) reading in metadata blob at the location
     * blobKey that describes the number of shards this data is split into, 2) asynchronously pull
     * in all sharded data, 3) merge sharded data and return result.
     *
     * @return A Flow of ByteString, where each emission represents a complete record from the
     *   RecordIO formatted data.
     * @throws kotlin.Exception If there is an issue reading metadata file.
     * @throws kotlin.Exception If there is an issue reading sharded data.
     */
    override fun read(): Flow<ByteString> {
      // 1. Read metadata blob to discover number of sharded files. Will be a string in the format
      // "sharded-impressions-*-of-N"
      val numShardsData = runBlocking {
        metadataBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
      }

      // Get total number of shards from blob with pattern {blobKey}-*-of-N where N is total number
      // of shards
      val numShards = numShardsData.split("-of-").last().toInt()

      // 2. Read in different files in parallel from underlying storage client
      val filePaths = (1..numShards).map { getChunkBlobKey(blobKey, it) }
      val shardedData: List<StorageClient.Blob> = runBlocking {
        filePaths
          .map {
            async {
              try {
                storageClient.getBlob(it)!!
              } catch (e: Exception) {
                throw Exception("Error retrieving blob at path: $it:", e)
              }
            }
          }
          .awaitAll()
      }

      // 3. Return a merged Flow<ByteString>
      return shardedData.map { it.read() }.merge()
    }

    override suspend fun delete() {
      storageClient.getBlob(blobKey)?.delete()
    }
  }

  companion object {
    private const val DEFAULT_CHUNK_SIZE = 50
    private const val CHUNK_METADATA_SUFFIX = "metadata"

    private fun getChunkBlobKey(blobKey: String, chunkIndex: Int): String {
      return "$blobKey/chunk-$chunkIndex"
    }
  }
}

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
class ShardedStorageClient(private val underlyingStorageClient: MesosRecordIoStorageClient): StorageClient {
  /**
   * Writes sharded data to MesosRecordIoStorage after it has been split into chunks.
   *
   * This function takes a flow of ByteString, splits it into chunks, and stores it in different
   * shards. The sharded data will be stored at the location blobKey-x-of-N, where x is the shard
   * index and N is the total number of shards. The data stored at the location blobKey is metadata
   * that describes the number of shards in the format "xxxxxx-*-of-N, where N is the total number
   * of shards.
   *
   * @param blobKey The key (or name) of the blob where the content will be stored.
   * @param content A Flow<ByteString> representing the source of RecordIO rows that will be stored.
   * @param chunkSize The size of the chunks that the data will be split into
   * @return A Blob object representing file that contains metadata on the sharded data the was
   *   written to storage. The file will be in the format "blobKey-*-of-N", where N represents the
   *   total number of shards the data was split into.
   */
  suspend fun writeBlob(blobKey: String, content: Flow<ByteString>, chunkSize: Int): StorageClient.Blob {
    val chunks = mutableListOf<Flow<ByteString>>()
    var currentChunk = mutableListOf<ByteString>()

    // Shard current Flow<ByteString> into smaller Flow<ByteString> chunks of size chunkSize
    content.collect { byteString ->
      currentChunk.add(byteString)
      if (currentChunk.size == chunkSize) {
        chunks.add(currentChunk.asFlow())
        currentChunk = mutableListOf()
      }
    }
    // Add any remaining elements as the last chunk
    if (currentChunk.isNotEmpty()) {
      chunks.add(currentChunk.asFlow())
    }

    // Write all smaller Flow<ByteString> chunks to storage using the same blob key concatinated
    // with the indexed entry of each shard with the pattern "-x-of-totalNumberOfChunks"
    runBlocking {
      chunks.mapIndexed { index, it ->
        async {
          writeBlob("$blobKey-${index + 1}-of-${chunks.size}", it)
        }
      }
    }

    // Write a string to storage that signifies the size of the sharded content and return Blob with this
    // metadata
    val shardData = listOf("$blobKey-*-of-${chunks.size}".toByteStringUtf8())
    return writeBlob(blobKey, shardData.asFlow())
  }

  /**
   * Writes data to storage.
   *
   * This function is not meant to be called directly. It should only be used through the method
   * writeBlob(blobKey: String, content: Flow<ByteString>, chunkSize: Int).
   *
   * @param blobKey The key (or name) of the blob where the content will be stored.
   * @param content A Flow<ByteString> representing the source of RecordIO rows that will be stored.
   * @return A Blob object representing file that was written to storage.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return underlyingStorageClient.writeBlob(blobKey, content)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is read as format when [Blob.read] is called.
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return Blob(blobKey, underlyingStorageClient, underlyingStorageClient.getBlob(blobKey)!!.size)
  }

  private inner class Blob(private val blobKey: String, override val storageClient: StorageClient, override val size: Long): StorageClient.Blob {
    /**
     * Reads data from MesosRecordIoStorage.
     *
     * This function reads sharded data from storage by 1) reading in metadata blob that describes
     * the number of shards this data is split over, 2) asynchronously pull in all sharded data,
     * 3) merge sharded data and return result.
     *
     *
     * @return A Flow of ByteString, where each emission represents a complete record from the
     *   RecordIO formatted data.
     * @throws kotlin.Exception If there is an issue reading metadata file.
     * @throws kotlin.Exception If there is an issue reading sharded data.
     */
    override fun read(): Flow<ByteString> {
      // 1. Read metadata blob to discover number of sharded files. Will be a string in the format "sharded-impressions-*-of-N"
      val metadata  = runBlocking {
        try {
          underlyingStorageClient.getBlob(blobKey)
        } catch (e: Exception) {
          throw Exception("Error retrieving blob metadata at path: $blobKey:", e)
        }
      }
      requireNotNull(metadata) {
        "No blob metadata found at path: $blobKey"
      }
      val numShardsData= runBlocking {
        metadata.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
      }

      // Get total number of shards from blob with pattern xxxxxxxx-*-of-N where N is total number of shards
      val numShards = numShardsData.split("-of-").last().toInt()

      // 2. Read in different files in parallel from underlying storage client
      val filePaths = (1..numShards).map { "$blobKey-$it-of-$numShards" }
      val shardedData = runBlocking {
        filePaths.map {
          async {
            try {
              storageClient.getBlob(it)!!
            } catch (e: Exception) {
              throw Exception("Error retrieving blob at path: $it:", e)
            }
          }
        }.awaitAll()
      }

      // 3. Return a merged Flow<ByteString>
      return shardedData.map {
        it.read()
      }.merge()
    }

    override suspend fun delete() {
      storageClient.getBlob(blobKey)?.delete()
    }
  }
}

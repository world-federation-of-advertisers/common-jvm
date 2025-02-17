package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.base64UrlDecode

class ShardedStorageClient(private val underlyingStorageClient: StorageClient): StorageClient {

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return Blob(blobKey, underlyingStorageClient, underlyingStorageClient.getBlob(blobKey)!!.size)
  }

  suspend fun writeBlob(blobKey: String, content: Flow<ByteString>, chunkSize: Int): StorageClient.Blob {
    val chunks = mutableListOf<Flow<ByteString>>()
    var currentChunk = mutableListOf<ByteString>()

    // Shard current Flow<ByteString> into smaller Flow<ByteString> elements of size chunkSize
    content.collect { byteString ->
      currentChunk.add(byteString)
      if (currentChunk.size == chunkSize) {
        chunks.add(currentChunk.asFlow())
        currentChunk = mutableListOf()
      }
    }

    // Write all smaller Flow<ByteString> shards to storage using the same blob key concatinated
    // with the indexed entry of each shard with the pattern "-x-of-totalNumberOfChunks"
    chunks.mapIndexed { index, it ->
      writeBlob("$blobKey-$index-of-${chunks.size}", it)
    }

    // Write a string to storage that signifies the size of the sharded content and return Blob with this
    // metadata
    val shardData = "$blobKey-*-of-${chunks.size}"
    return writeBlob(blobKey, shardData.base64UrlDecode())
  }
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return underlyingStorageClient.writeBlob(blobKey, content)
  }
  private inner class Blob(private val blobKey: String, override val storageClient: StorageClient, override val size: Long): StorageClient.Blob {
    override fun read(): Flow<ByteString> {
      // 1. Read original file to discover number of sharded files. Will be a string in the format "sharded-impressions-*-of-N"
      val metadata  = runBlocking {
        underlyingStorageClient.getBlob(blobKey)!! // /{prefix}/ds/{ds}/event-group-id/{event-group-id}/sharded-impressions
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
              storageClient.getBlob(it)!!.read()
            } catch (e: Exception) {
              throw Exception("Error retrieving blob at path: $it:", e)
            }
          }
        }.awaitAll()
      }

      // 3. Return a merged Flow<ByteString>
      return  shardedData.asFlow().flattenMerge()
    }

    override suspend fun delete() {
      storageClient.getBlob(blobKey)?.delete()
    }
  }
}

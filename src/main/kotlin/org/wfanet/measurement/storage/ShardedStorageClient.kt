package org.wfanet.measurement.storage

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.runBlocking

class ShardedStorageClient(private val underlyingStorageClient: StorageClient): StorageClient {

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return Blob(blobKey, underlyingStorageClient, underlyingStorageClient.getBlob(blobKey)!!.size)
  }

  suspend fun writeBlob(blobKey: String, content: Flow<ByteString>, chunkSize: Int): StorageClient.Blob {
    val chunks = mutableListOf<Flow<ByteString>>()
    var currentChunk = mutableListOf<ByteString>()

    // Shard current Flow<ByteString> into smaller Flow<ByteString> chunks of size chunkSize
    content.collect { byteString ->
      val dataSize: String = byteString.size().toString()
      val separatedChunk: String = dataSize + "\n" + byteString.toStringUtf8()
      currentChunk.add(separatedChunk.toByteStringUtf8())
      if (currentChunk.size == chunkSize) {
        chunks.add(currentChunk.asFlow())
        currentChunk = mutableListOf<ByteString>()
      }
    }
    // Add any remaining elements as the last chunk
    if (currentChunk.isNotEmpty()) {
      chunks.add(currentChunk.asFlow())
    }

    // Write all smaller Flow<ByteString> chunks to storage using the same blob key concatinated
    // with the indexed entry of each shard with the pattern "-x-of-totalNumberOfChunks"
    chunks.mapIndexed { index, it ->
      writeBlob("$blobKey-${index+1}-of-${chunks.size}", it)
    }

    // Write a string to storage that signifies the size of the sharded content and return Blob with this
    // metadata
    val shardData = listOf("$blobKey-*-of-${chunks.size}".toByteStringUtf8())
    return writeBlob(blobKey, shardData.asFlow())
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
              storageClient.getBlob(it)!!
            } catch (e: Exception) {
              throw Exception("Error retrieving blob at path: $it:", e)
            }
          }
        }.awaitAll()
      }

      // 3. Return a merged Flow<ByteString>
      return flow {
        shardedData.map {
          val recordSizeBuffer = StringBuilder()
          var currentRecordSize = -1
          var recordBuffer = ByteString.newOutput()

          it.read().collect { chunk ->
            var position = 0
            val chunkString = chunk.toStringUtf8()

            while (position < chunkString.length) {
              if (currentRecordSize == -1) {

                val newlineIndex = chunkString.indexOf("\n", position)
                if (newlineIndex != -1) {
                  recordSizeBuffer.append(chunkString.substring(position, newlineIndex))
                  currentRecordSize = recordSizeBuffer.toString().toInt()
                  recordSizeBuffer.clear()
                  recordBuffer = ByteString.newOutput(currentRecordSize)
                  position = newlineIndex + 1
                } else {
                  recordSizeBuffer.append(chunkString.substring(position))
                  break
                }
              }
              if (currentRecordSize > 0) {
                val remainingBytes = chunkString.length - position
                val bytesToRead = minOf(remainingBytes, currentRecordSize - recordBuffer.size())

                if (bytesToRead > 0) {
                  recordBuffer.write(
                    chunkString.substring(position, position + bytesToRead).toByteArray(Charsets.UTF_8)
                  )
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
      }
    }

    override suspend fun delete() {
      storageClient.getBlob(blobKey)?.delete()
    }
  }
}

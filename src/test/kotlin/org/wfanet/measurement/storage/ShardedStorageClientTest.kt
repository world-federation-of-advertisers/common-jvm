package org.wfanet.measurement.storage

import com.google.common.truth.Truth
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

class ShardedStorageClientTest {
  private lateinit var wrappedStorageClient: StorageClient
  private lateinit var shardedtorageClient: ShardedStorageClient

  @Before
  fun initStorageClient() {
    wrappedStorageClient = InMemoryStorageClient()
    shardedtorageClient = ShardedStorageClient(wrappedStorageClient)
  }

  @Test
  fun `test writing and reading sharded record`() = runBlocking {
    val testData = listOf("impression1", "impression2", "impression3", "impression4")
    val blobKey = "/labelled-impressions/ds/2025-02-14/event-group-id/12345/sharded-impressions"

    shardedtorageClient.writeBlob(
      blobKey,
      testData.map { it.toByteStringUtf8() }.asFlow(),
      3
    )
    val blob = shardedtorageClient.getBlob(blobKey)
    requireNotNull(blob) { "Blob should exist" }
    val records = blob.read().toList().map { it.toStringUtf8().toString() }.toString()
    Truth.assertThat(testData.toString()).isEqualTo(records)
  }
}

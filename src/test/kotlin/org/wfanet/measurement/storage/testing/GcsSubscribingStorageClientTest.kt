/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.storage.testing

import com.google.cloud.functions.CloudEventsFunction
import com.google.common.truth.Truth.assertThat
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.kotlin.toByteStringUtf8
import io.cloudevents.CloudEvent
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class GcsSubscribingStorageClientTest : AbstractStorageClientTest<InMemoryStorageClient>() {
  @Before
  fun initStorageClient() {
    storageClient = InMemoryStorageClient()
  }

  @Test
  fun `subscribe`() = runBlocking {
    val underlyingClient = InMemoryStorageClient()
    val client = GcsSubscribingStorageClient(underlyingClient)
    val mockCloudFunction: CloudEventsFunction = mock {
      whenever(it.accept(any())).then { print("Accept!!!!\n\n\n\nAccept!!!!") }
    }

    val blobKey = "some-blob-key"
    val contents = "some-contents".toByteStringUtf8()

    client.subscribe(mockCloudFunction)

    client.writeBlob(blobKey, contents)

    val cloudEventCaptor = argumentCaptor<CloudEvent>()
    verify(mockCloudFunction, times(1)).accept(cloudEventCaptor.capture())

    val data = StorageObjectData.newBuilder().setName(blobKey).setBucket("fake-bucket").build()
    assertThat(StorageObjectData.parseFrom(cloudEventCaptor.firstValue.data!!.toBytes()))
      .isEqualTo(data)
  }
}

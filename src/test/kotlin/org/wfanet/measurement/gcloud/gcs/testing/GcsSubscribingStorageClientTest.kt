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

package org.wfanet.measurement.gcloud.gcs.testing

import com.google.cloud.functions.CloudEventsFunction
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest

@RunWith(JUnit4::class)
class GcsSubscribingStorageClientTest : AbstractStorageClientTest<GcsStorageClient>() {
  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Test
  fun `writeBlob publishes finalize event to subscribers`() = runBlocking {
    val client = GcsSubscribingStorageClient(storageClient)
    val mockCloudFunction: CloudEventsFunction = mock {}

    val blobKey = "some-blob-key"
    val contents = "some-contents".toByteStringUtf8()
    val anotherBlobKey = "other-blob-key"
    val otherContents = "other-contents".toByteStringUtf8()
    client.subscribe(mockCloudFunction)

    client.writeBlob(blobKey, contents)
    client.writeBlob(anotherBlobKey, otherContents)

    val cloudEventCaptor = argumentCaptor<CloudEvent>()
    verify(mockCloudFunction, times(2)).accept(cloudEventCaptor.capture())

    val acceptedData =
      cloudEventCaptor.allValues
        .map { parseStorageObjectData(it) }
        .map { Pair(it!!.name, it.bucket) }
    assertThat(acceptedData)
      .containsExactlyElementsIn(
        listOf(Pair("some-blob-key", BUCKET), Pair("other-blob-key", BUCKET))
      )
      .inOrder()
  }

  /*
   * Based on java example from https://cloud.google.com/functions/docs/samples/functions-cloudevent-storage
   */
  private fun parseStorageObjectData(event: CloudEvent): StorageObjectData? {
    val eventData: CloudEventData = event.data ?: return null
    val storageObjectDataJson = eventData.toBytes().decodeToString()

    // If you do not ignore unknown fields, then JsonFormat.Parser returns an
    // error when encountering a new or unknown field. Note that you might lose
    // some event data in the unmarshaling process by ignoring unknown fields.
    val parser = JsonFormat.parser().ignoringUnknownFields()
    return StorageObjectData.newBuilder()
      .apply { parser.merge(storageObjectDataJson, this) }
      .build()
  }

  companion object {
    private const val BUCKET = "test-bucket"
  }
}

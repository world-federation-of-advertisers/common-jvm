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

package org.wfanet.measurement.gcloud.gcs.testing

import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.ByteString
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import java.net.URI
import java.time.Clock
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Used for local testing of Cloud Run function triggered by upload to Cloud Storage. Emulates
 * https://cloud.google.com/functions/docs/samples/functions-cloudevent-storage-unit-test
 */
class GcsSubscribingStorageClient(
    private val storageClient: GcsStorageClient,
    private val clock: Clock = Clock.systemUTC(),
) : StorageClient {
  private var subscribingFunctions = mutableListOf<CloudEventsFunction>()

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val blob = storageClient.writeBlob(blobKey, content)

    // Get current system timestamp.
    val timestamp = clock.protoTimestamp()

    val data =
        StorageObjectData.newBuilder()
            .setName(blobKey)
            .setBucket(storageClient.bucketName)
            .setMetageneration(10)
            .setTimeCreated(timestamp)
            .setUpdated(timestamp)
            .build()

    val event: CloudEvent =
        CloudEventBuilder.v1()
            .withId("some-id")
            .withSource(URI.create("some-uri"))
            .withType("google.storage.object.finalize")
            .withData("application/json", data.toJson().toByteArray())
            .build()
    subscribingFunctions.forEach { subscribingFunction ->
      logger.fine { "Sending $blobKey to function $subscribingFunction" }
      subscribingFunction.accept(event)
    }
    return blob
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return storageClient.getBlob(blobKey)
  }

  fun subscribe(function: CloudEventsFunction) {
    subscribingFunctions.add(function)
  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
    const val FAKE_BUCKET = "fake-bucket"
  }
}

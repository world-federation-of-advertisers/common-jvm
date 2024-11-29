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

package org.wfanet.measurement.gcloud.pubsub

import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.protobuf.Message
import com.google.pubsub.v1.PubsubMessage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.queue.QueuePublisher

/**
 * A class for publishing messages to Google Cloud Pub/Sub topics.
 *
 * @property projectId The Google Cloud project ID.
 * @property googlePubSubClient The client used for managing Pub/Sub resources. Defaults to
 *   [DefaultGooglePubSubClient].
 */
class Publisher(
  val projectId: String,
  val googlePubSubClient: GooglePubSubClient = DefaultGooglePubSubClient(),
) : QueuePublisher {

  /** A concurrent map to store and reuse [Publisher] instances by topic ID. */
  private val publishers = ConcurrentHashMap<String, GooglePublisher>()

  /**
   * Publishes a message to the specified Pub/Sub topic.
   *
   * @param topicId The ID of the topic to publish the message to.
   * @param message The [Message] to be published.
   * @throws Exception If there is an error while publishing the message.
   */
  override suspend fun publishMessage(topicId: String, message: Message) {

    val pubsubPublisher =
      publishers.computeIfAbsent(topicId) { googlePubSubClient.buildPublisher(projectId, topicId) }

    val pubsubMessage = PubsubMessage.newBuilder().setData(message.toByteString()).build()
    pubsubPublisher.publish(pubsubMessage).await()
  }

  override fun close() {
    publishers.forEach { (_, pubsubPublisher) ->
      pubsubPublisher.shutdown()
      pubsubPublisher.awaitTermination(5, TimeUnit.SECONDS)
    }
    publishers.clear()
  }
}

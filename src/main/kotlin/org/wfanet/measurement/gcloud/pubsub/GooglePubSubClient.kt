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

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.cloud.pubsub.v1.Subscriber as GoogleSubscriber
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.pubsub.v1.DeleteTopicRequest
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.Topic
import com.google.pubsub.v1.TopicName
import org.threeten.bp.Duration
import org.wfanet.measurement.gcloud.common.await

/** Abstract base class for managing Google Cloud Pub/Sub resources and interactions. */
abstract class GooglePubSubClient : AutoCloseable {

  private val topicAdminClient: Lazy<TopicAdminClient> = lazy { buildTopicAdminClient() }

  /**
   * Builds the TopicAdminClient used to manage Pub/Sub topics.
   *
   * @return A new instance of [TopicAdminClient].
   */
  protected abstract fun buildTopicAdminClient(): TopicAdminClient

  /**
   * Builds a Google Pub/Sub [GoogleSubscriber].
   *
   * @param projectId The Google Cloud project ID.
   * @param subscriptionId The ID of the subscription to receive messages from.
   * @param ackExtensionPeriod The duration for which the acknowledgment deadline is extended while
   *   processing a message. This defines the time period during which the message will not be
   *   re-delivered if neither an acknowledgment (ack) nor a negative acknowledgment (nack) is
   *   received.
   * @param messageHandler A callback function invoked for each message received. It provides the
   *   [PubsubMessage] and an [AckReplyConsumer] to acknowledge or negatively acknowledge the
   *   message.
   * @return A [GoogleSubscriber] instance.
   */
  abstract fun buildSubscriber(
    projectId: String,
    subscriptionId: String,
    ackExtensionPeriod: Duration,
    messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit,
  ): GoogleSubscriber

  /**
   * Builds a Google Pub/Sub [GooglePublisher].
   *
   * @param projectId The Google Cloud project ID.
   * @param topicId The ID of the topic to publish messages to.
   * @return A [GooglePublisher] instance.
   */
  abstract fun buildPublisher(projectId: String, topicId: String): GooglePublisher

  suspend fun createTopic(projectId: String, topicId: String): TopicName {
    val topicName = TopicName.of(projectId, topicId)
    val topic = Topic.newBuilder().setName(topicName.toString()).build()
    topicAdminClient.value.createTopicCallable().futureCall(topic).await()
    return topicName
  }

  suspend fun deleteTopic(projectId: String, topicId: String) {
    val topicName = TopicName.of(projectId, topicId)
    val request = DeleteTopicRequest.newBuilder().setTopic(topicName.toString()).build()
    topicAdminClient.value.deleteTopicCallable().futureCall(request).await()
  }

  override fun close() {
    if (topicAdminClient.isInitialized()) {
      topicAdminClient.value.close()
    }
  }
}

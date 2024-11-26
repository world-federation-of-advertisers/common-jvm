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
import com.google.cloud.pubsub.v1.Subscriber
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.threeten.bp.Duration

interface GooglePubSubClient {

  /**
   * Builds a subscriber for the specified Pub/Sub subscription.
   *
   * @param projectId The Google Cloud project ID where the subscription resides.
   * @param subscriptionId The ID of the Pub/Sub subscription to which the subscriber will listen.
   * @param ackExtensionPeriod The duration for which the acknowledgment deadline is extended while
   *   processing a message. This defines the time period during which the message will not be
   *   re-delivered if neither an acknowledgment (ack) nor a negative acknowledgment (nack) is
   *   received.
   * @param messageHandler A callback function invoked for each message received. It provides the
   *   [PubsubMessage] and an [AckReplyConsumer] to acknowledge or negatively acknowledge the
   *   message.
   * @return A [Subscriber] instance configured with the provided parameters.
   */
  fun buildSubscriber(
    projectId: String,
    subscriptionId: String,
    ackExtensionPeriod: Duration,
    messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit,
  ): Subscriber

  /**
   * Publishes a message to the specified Pub/Sub topic.
   *
   * @param projectId The Google Cloud project ID where the topic resides.
   * @param topicId The ID of the Pub/Sub topic to which the message will be published.
   * @param messageContent The content of the message to be published, as a [ByteString].
   */
  fun publishMessage(projectId: String, topicId: String, messageContent: ByteString)

  /**
   * Checks if a Pub/Sub topic exists in the specified project.
   *
   * @param projectId The Google Cloud project ID where the topic is expected to exist.
   * @param topicId The ID of the Pub/Sub topic to check.
   * @return `true` if the topic exists, otherwise `false`.
   */
  fun topicExists(projectId: String, topicId: String): Boolean
}

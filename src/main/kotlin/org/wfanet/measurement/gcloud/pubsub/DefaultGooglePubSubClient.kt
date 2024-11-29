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
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.cloud.pubsub.v1.Subscriber as GoogleSubscriber
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import org.threeten.bp.Duration

class DefaultGooglePubSubClient : GooglePubSubClient() {
  override fun buildTopicAdminClient(): TopicAdminClient {
    return TopicAdminClient.create(TopicAdminSettings.newBuilder().build())
  }

  override fun buildSubscriptionAdminClient(): SubscriptionAdminClient {
    return SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder().build())
  }

  override fun buildSubscriber(
    projectId: String,
    subscriptionId: String,
    ackExtensionPeriod: Duration,
    messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit,
  ): GoogleSubscriber {

    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    val messageReceiver = MessageReceiver { message, consumer -> messageHandler(message, consumer) }
    val subscriberBuilder =
      GoogleSubscriber.newBuilder(subscriptionName, messageReceiver)
        .setMaxAckExtensionPeriod(ackExtensionPeriod)

    return subscriberBuilder.build()
  }

  override fun buildPublisher(projectId: String, topicId: String): GooglePublisher {
    val topicName = TopicName.of(projectId, topicId)
    return GooglePublisher.newBuilder(topicName).build()
  }
}

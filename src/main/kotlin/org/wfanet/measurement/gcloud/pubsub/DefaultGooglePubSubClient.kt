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

import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.protobuf.Message
import com.google.pubsub.v1.GetTopicRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import org.threeten.bp.Duration
import org.wfanet.measurement.gcloud.common.await

class DefaultGooglePubSubClient : GooglePubSubClient {

  override fun buildSubscriber(
    projectId: String,
    subscriptionId: String,
    ackExtensionPeriod: Duration,
    messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit,
  ): Subscriber {

    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    val messageReceiver = MessageReceiver { message, consumer -> messageHandler(message, consumer) }
    val subscriberBuilder =
      Subscriber.newBuilder(subscriptionName, messageReceiver)
        .setMaxAckExtensionPeriod(ackExtensionPeriod)

    return subscriberBuilder.build()
  }

  override suspend fun publishMessage(projectId: String, topicId: String, message: Message) {
    val topicName = TopicName.of(projectId, topicId)
    val publisher = Publisher.newBuilder(topicName).build()
    try {
      val pubsubMessage = PubsubMessage.newBuilder().setData(message.toByteString()).build()
      publisher.publish(pubsubMessage).await()
    } finally {
      publisher.shutdown()
    }
  }

  override suspend fun topicExists(projectId: String, topicId: String): Boolean {
    return TopicAdminClient.create().use { topicAdminClient ->
      try {
        val request: GetTopicRequest =
          GetTopicRequest.newBuilder().setTopic(TopicName.of(projectId, topicId).toString()).build()
        topicAdminClient.getTopicCallable().futureCall(request).await()
        true
      } catch (e: NotFoundException) {
        false
      }
    }
  }
}

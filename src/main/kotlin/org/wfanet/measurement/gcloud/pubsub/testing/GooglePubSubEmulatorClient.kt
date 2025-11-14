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

package org.wfanet.measurement.gcloud.pubsub.testing

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.pubsub.v1.DeleteSubscriptionRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.Subscription
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannelBuilder
import org.threeten.bp.Duration
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient

/**
 * A client for managing a Google Pub/Sub emulator, providing utilities to interact with topics and
 * subscriptions in an emulated environment.
 */
class GooglePubSubEmulatorClient(host: String, port: Int) : GooglePubSubClient() {

  private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
  private val channelProvider =
    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
  private val credentialsProvider = NoCredentialsProvider.create()

  override fun buildTopicAdminClient(): TopicAdminClient {
    return TopicAdminClient.create(
      TopicAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )
  }

  override fun buildSubscriptionAdminClient(): SubscriptionAdminClient {
    return SubscriptionAdminClient.create(
      SubscriptionAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )
  }

  override fun buildPublisher(projectId: String, topicId: String): Publisher {
    val topicName = TopicName.of(projectId, topicId)
    return Publisher.newBuilder(topicName)
      .setChannelProvider(channelProvider)
      .setCredentialsProvider(credentialsProvider)
      .build()
  }

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
        .setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .setMaxAckExtensionPeriod(ackExtensionPeriod)

    return subscriberBuilder.build()
  }

  suspend fun createSubscription(projectId: String, subscriptionId: String, topicId: String) {
    val topicName = TopicName.of(projectId, topicId)
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    val subscription =
      Subscription.newBuilder()
        .setName(subscriptionName)
        .setTopic(topicName.toString())
        .setPushConfig(PushConfig.getDefaultInstance())
        .build()

    subscriptionAdminClient.value.createSubscriptionCallable().futureCall(subscription).await()
  }

  suspend fun deleteSubscription(projectId: String, subscriptionId: String) {
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    val request = DeleteSubscriptionRequest.newBuilder().setSubscription(subscriptionName).build()
    subscriptionAdminClient.value.deleteSubscriptionCallable().futureCall(request).await()
  }

  override fun close() {
    super.close()
    channel.shutdown()
  }
}

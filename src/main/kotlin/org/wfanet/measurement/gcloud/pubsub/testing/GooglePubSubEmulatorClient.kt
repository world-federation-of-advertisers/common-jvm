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
import com.google.api.gax.rpc.NotFoundException
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.protobuf.Message
import com.google.pubsub.v1.DeleteSubscriptionRequest
import com.google.pubsub.v1.DeleteTopicRequest
import com.google.pubsub.v1.GetTopicRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.Subscription
import com.google.pubsub.v1.Topic
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.threeten.bp.Duration
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient

/**
 * A client for managing a Google Pub/Sub emulator, providing utilities to interact with topics and
 * subscriptions in an emulated environment.
 */
class GooglePubSubEmulatorClient(private val host: String, private val port: Int) :
  GooglePubSubClient, AutoCloseable {

  private val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
  private val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
  private val credentialsProvider = NoCredentialsProvider.create()
  private val topicAdminClient =
    TopicAdminClient.create(
      TopicAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )
  private val subscriptionAdminClient =
    SubscriptionAdminClient.create(
      SubscriptionAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )

  suspend fun createTopic(projectId: String, topicId: String): TopicName {
    val topicName = TopicName.of(projectId, topicId)
    val topic = Topic.newBuilder().setName(topicName.toString()).build()
    topicAdminClient.createTopicCallable().futureCall(topic).await()
    return topicName
  }

  suspend fun deleteTopic(projectId: String, topicId: String) {
    val topicName = TopicName.of(projectId, topicId)
    val request = DeleteTopicRequest.newBuilder().setTopic(topicName.toString()).build()
    topicAdminClient.deleteTopicCallable().futureCall(request).await()
  }

  suspend fun createSubscription(projectId: String, subscriptionId: String, topicId: String) {
    val topicName = TopicName.of(projectId, topicId)
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    val subscription =
      Subscription.newBuilder()
        .setName(subscriptionName)
        .setTopic(topicName.toString())
        .setPushConfig(PushConfig.getDefaultInstance())
        .setAckDeadlineSeconds(10)
        .build()

    subscriptionAdminClient.createSubscriptionCallable().futureCall(subscription).await()
  }

  suspend fun deleteSubscription(projectId: String, subscriptionId: String) {
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    val request = DeleteSubscriptionRequest.newBuilder().setSubscription(subscriptionName).build()
    subscriptionAdminClient.deleteSubscriptionCallable().futureCall(request).await()
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

  override suspend fun publishMessage(projectId: String, topicId: String, message: Message) {
    val topicName = TopicName.of(projectId, topicId)
    val publisher =
      Publisher.newBuilder(topicName)
        .setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    val pubsubMessage = PubsubMessage.newBuilder().setData(message.toByteString()).build()
    publisher.publish(pubsubMessage).await()
  }

  override suspend fun topicExists(projectId: String, topicId: String): Boolean {
    return try {
      val request: GetTopicRequest =
        GetTopicRequest.newBuilder().setTopic(TopicName.of(projectId, topicId).toString()).build()
      topicAdminClient.getTopicCallable().futureCall(request).await()
      true
    } catch (e: NotFoundException) {
      false
    }
  }

  override fun close() {
    channel.shutdown()
  }
}

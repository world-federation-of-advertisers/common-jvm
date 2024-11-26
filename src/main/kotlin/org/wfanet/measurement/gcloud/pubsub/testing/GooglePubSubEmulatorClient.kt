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

import com.google.api.gax.core.CredentialsProvider
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
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.Subscription
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName
import org.threeten.bp.Duration
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient

/**
 * A client for managing a Google Pub/Sub emulator, providing utilities to interact with topics and
 * subscriptions in an emulated environment.
 *
 * This class uses Testcontainers to run the Google Pub/Sub emulator.
 */
class GooglePubSubEmulatorClient : GooglePubSubClient {

  private val PUBSUB_IMAGE_NAME = "gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"

  private lateinit var pubsubEmulator: PubSubEmulatorContainer
  private lateinit var channel: ManagedChannel
  private lateinit var channelProvider: TransportChannelProvider
  private lateinit var credentialsProvider: CredentialsProvider
  private lateinit var topicAdminClient: TopicAdminClient
  private lateinit var subscriptionAdminClient: SubscriptionAdminClient

  fun startEmulator() {
    pubsubEmulator = PubSubEmulatorContainer(DockerImageName.parse(PUBSUB_IMAGE_NAME))
    pubsubEmulator.start()
    initializeChannel()
    initializeClients()
  }

  fun stopEmulator() {
    if (::pubsubEmulator.isInitialized) {
      pubsubEmulator.stop()
    }
    if (::channel.isInitialized) {
      channel.shutdown()
    }
  }

  /** Initializes the gRPC channel and sets up the transport and credentials providers. */
  private fun initializeChannel() {
    channel =
      ManagedChannelBuilder.forAddress(pubsubEmulator.host, pubsubEmulator.getMappedPort(8085))
        .usePlaintext()
        .build()
    channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
    credentialsProvider = NoCredentialsProvider.create()
  }

  /** Initializes the topic and subscription admin clients for managing Pub/Sub resources. */
  private fun initializeClients() {
    topicAdminClient =
      TopicAdminClient.create(
        TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build()
      )
    subscriptionAdminClient =
      SubscriptionAdminClient.create(
        SubscriptionAdminSettings.newBuilder()
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build()
      )
  }

  fun createTopic(projectId: String, topicId: String): TopicName {
    val topicName = TopicName.of(projectId, topicId)
    topicAdminClient.createTopic(topicName)
    return topicName
  }

  fun deleteTopic(projectId: String, topicId: String) {
    val topicName = TopicName.of(projectId, topicId)
    topicAdminClient.deleteTopic(topicName)
  }

  fun createSubscription(projectId: String, subscriptionId: String, topicId: String) {
    val topicName = TopicName.of(projectId, topicId)
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    subscriptionAdminClient.createSubscription(
      Subscription.newBuilder()
        .setName(subscriptionName)
        .setTopic(topicName.toString())
        .setPushConfig(PushConfig.getDefaultInstance())
        .setAckDeadlineSeconds(10)
        .build()
    )
  }

  fun deleteSubscription(projectId: String, subscriptionId: String) {
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    subscriptionAdminClient.deleteSubscription(subscriptionName)
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

  override fun publishMessage(projectId: String, topicId: String, messageContent: ByteString) {
    val topicName = TopicName.of(projectId, topicId)
    val publisher =
      Publisher.newBuilder(topicName)
        .setChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    val pubsubMessage = PubsubMessage.newBuilder().setData(messageContent).build()
    publisher.publish(pubsubMessage).get()
  }

  override fun topicExists(projectId: String, topicId: String): Boolean {
    val topicName = TopicName.of(projectId, topicId)
    return try {
      topicAdminClient.getTopic(topicName)
      true
    } catch (e: NotFoundException) {
      false
    }
  }
}

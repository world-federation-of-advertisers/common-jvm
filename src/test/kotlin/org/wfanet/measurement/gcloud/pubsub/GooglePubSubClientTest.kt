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

import com.google.api.core.ApiFuture
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.pubsub.v1.Subscription
import io.grpc.ManagedChannelBuilder
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import org.wfa.measurement.common.rabbitmq.TestWork
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PushConfig
import com.google.common.truth.Truth
import io.grpc.ManagedChannel

class GooglePubSubClientTest {

  private val projectId = "xab-prod-test" // "test-project"
  private val subscriptionId = "test-subscription"
  private val topicId = "test-topic"

  private lateinit var pubSubClient: GooglePubSubClient
  private lateinit var publisher: Publisher
  private lateinit var topicAdminClient: TopicAdminClient
  private lateinit var subscriptionAdminClient: SubscriptionAdminClient
  private lateinit var subscriberStubSettings: SubscriberStubSettings
  private lateinit var channel: ManagedChannel

  fun isHostReachable(host: String, port: Int, timeoutMillis: Int = 1000): Boolean {
    return try {
      Socket().use { socket ->
        socket.connect(InetSocketAddress(host, port), timeoutMillis)
      }
      true
    } catch (e: Exception) {
      when (e) {
        is SocketTimeoutException -> println("Connection timed out.")
        else -> println("Connection failed: ${e.message}")
      }
      false
    }
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder()
      .setUserName(message)
      .setUserAge("25")
      .setUserCountry("US")
      .build()
  }

  @Before
  fun setup() {
//    val hostport = System.getenv("PUBSUB_EMULATOR_HOST")
    val hostport = "localhost:8085"

    channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build()
    val channelProvider: TransportChannelProvider =
      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))


      val credentialsProvider: CredentialsProvider = NoCredentialsProvider.create()

      topicAdminClient =
        TopicAdminClient.create(
          TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build()
        )
      val topicName: TopicName = TopicName.of(projectId, topicId)
      topicAdminClient.createTopic(topicName)


      publisher =
        Publisher.newBuilder(topicName)
          .setChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build()

      subscriberStubSettings = SubscriberStubSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()


      val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
      subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build())
      subscriptionAdminClient.createSubscription(
          Subscription.newBuilder()
            .setName(subscriptionName)
            .setTopic(topicName.toString())
            .setPushConfig(PushConfig.getDefaultInstance())
            .setAckDeadlineSeconds(10)
            .build()
        )

  }

  @After
  fun tearDown() {
    runBlocking {
      topicAdminClient.deleteTopic(TopicName.of(projectId, topicId))
      subscriptionAdminClient.deleteSubscription(ProjectSubscriptionName.format(projectId, subscriptionId))
      pubSubClient.close()
      channel.shutdown()
    }
  }

  @Test
  fun `should receive and ack message`() {

    runBlocking {
      val messages = listOf("UserName1", "UserName2", "UserName3")
      messages.forEach { msg ->
        val pubsubMessage: PubsubMessage =
          PubsubMessage.newBuilder().setData(createTestWork(msg).toByteString()).build()

        val messageIdFuture: ApiFuture<String> = publisher.publish(pubsubMessage)
        val messageId = messageIdFuture.get()
      }


      val subscriberStub: GrpcSubscriberStub = GrpcSubscriberStub.create(subscriberStubSettings)

      pubSubClient = GooglePubSubClient(
        projectId = projectId,
        subscriberStub = subscriberStub
      )

      val receivedMessages = mutableListOf<String>()
      val messageChannel = pubSubClient.subscribe<TestWork>(subscriptionId, TestWork.parser())
      while (receivedMessages.size < messages.size) {
        val message = messageChannel.receive()
        receivedMessages.add(message.body.userName)
        message.ack()
      }

      Truth.assertThat(receivedMessages).containsExactlyElementsIn(messages)

    }
  }

    @Test
    fun `should receive and nack message`() {

      runBlocking {
        val testWork = TestWork.newBuilder()
          .setUserName("Kevin")
          .setUserAge("25")
          .setUserCountry("US")
          .build()

        val messages = listOf("UserName1")
        messages.forEach { msg ->
          val pubsubMessage: PubsubMessage =
            PubsubMessage.newBuilder().setData(createTestWork(msg).toByteString()).build()

          val messageIdFuture: ApiFuture<String> = publisher.publish(pubsubMessage)
          val messageId = messageIdFuture.get()
          println("Published message ID: $messageId")
        }


        val subscriberStub: GrpcSubscriberStub = GrpcSubscriberStub.create(subscriberStubSettings)

        pubSubClient = GooglePubSubClient(
          projectId = projectId,
          subscriberStub = subscriberStub
        )

        val receivedMessages = mutableListOf<String>()
        val seenMessages = mutableSetOf<String>()
        val messageChannel = pubSubClient.subscribe<TestWork>(subscriptionId, TestWork.parser())
        while (receivedMessages.size < messages.size) {
          val message = messageChannel.receive()
          val userName = message.body.userName
          if (userName in seenMessages) {
            message.ack()
            receivedMessages.add(userName)
          } else {
            message.nack()
            seenMessages.add(userName)
          }

        }

        Truth.assertThat(receivedMessages).containsExactlyElementsIn(messages)

      }
    }

  }

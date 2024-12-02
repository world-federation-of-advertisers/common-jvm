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

import com.google.common.truth.Truth
import com.google.pubsub.v1.PubsubMessage
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfa.measurement.queue.testing.TestWork
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider

class SubscriberTest {

  @Rule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private val projectId = "test-project"
  private val subscriptionId = "test-subscription"
  private val topicId = "test-topic"

  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  @Before
  fun setup() {
    runBlocking {
      emulatorClient =
        GooglePubSubEmulatorClient(
          host = pubSubEmulatorProvider.host,
          port = pubSubEmulatorProvider.port,
        )
      emulatorClient.createTopic(projectId, topicId)
      emulatorClient.createSubscription(projectId, subscriptionId, topicId)
    }
  }

  @After
  fun tearDown() {
    runBlocking {
      emulatorClient.deleteTopic(projectId, topicId)
      emulatorClient.deleteSubscription(projectId, subscriptionId)
    }
  }

  @Test
  fun `should receive and ack message`() {

    runBlocking {
      val messages = listOf("UserName1", "UserName2", "UserName3")
      publishMessage(messages)
      val subscriber = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)

      val receivedMessages = mutableListOf<String>()
      val messageChannel = subscriber.subscribe<TestWork>(subscriptionId, TestWork.parser())
      while (receivedMessages.size < messages.size) {
        val message = messageChannel.receive()
        receivedMessages.add(message.body.userName)
        message.ack()
      }
      Truth.assertThat(receivedMessages).containsExactlyElementsIn(messages)
    }
  }

  @Test
  fun `message sould be published again after nack`() {

    runBlocking {
      val messages = listOf("UserName1")
      publishMessage(messages)
      val subscriber = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)

      val receivedMessages = mutableListOf<String>()
      val seenMessages = mutableSetOf<String>()
      val messageChannel = subscriber.subscribe<TestWork>(subscriptionId, TestWork.parser())
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

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().setUserName(message).setUserAge("25").setUserCountry("US").build()
  }

  private suspend fun publishMessage(messages: List<String>) {
    val publisher = emulatorClient.buildPublisher(projectId, topicId)
    messages.forEach { msg ->
      val pubsubMessage =
        PubsubMessage.newBuilder().setData(createTestWork(msg).toByteString()).build()
      publisher.publish(pubsubMessage)
    }
    publisher.shutdown()
    publisher.awaitTermination(5, TimeUnit.SECONDS)
  }
}

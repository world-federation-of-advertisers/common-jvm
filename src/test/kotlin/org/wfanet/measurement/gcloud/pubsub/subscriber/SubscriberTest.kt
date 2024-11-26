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

package org.wfanet.measurement.gcloud.pubsub.subscriber

import com.google.common.truth.Truth
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.wfa.measurement.queue.TestWork
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient

public class SubscriberTest : AutoCloseable {

  private val projectId = "test-project"
  private val subscriptionId = "test-subscription"
  private val topicId = "test-topic"

  private val emulatorClient: GooglePubSubEmulatorClient

  init {
    emulatorClient = GooglePubSubEmulatorClient()
    emulatorClient.startEmulator()
  }

  private lateinit var pubSubClient: Subscriber

  @Before
  fun setup() {
    emulatorClient.createSubscription(projectId, subscriptionId, topicId)
  }

  @After
  fun tearDown() {
    emulatorClient.deleteTopic(projectId, topicId)
    emulatorClient.deleteSubscription(projectId, subscriptionId)
  }

  @Test
  fun `should receive and ack message`() {

    runBlocking {
      val messages = listOf("UserName1", "UserName2", "UserName3")
      publishMessage(messages)

      pubSubClient = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)

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
  fun `message sould be published again after nack`() {

    runBlocking {
      val messages = listOf("UserName1")
      publishMessage(messages)

      pubSubClient = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)

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

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().setUserName(message).setUserAge("25").setUserCountry("US").build()
  }

  private fun publishMessage(messages: List<String>) {
    messages.forEach { msg ->
      emulatorClient.publishMessage(projectId, topicId, createTestWork(msg).toByteString())
    }
  }

  override fun close() {
    emulatorClient.stopEmulator()
  }
}

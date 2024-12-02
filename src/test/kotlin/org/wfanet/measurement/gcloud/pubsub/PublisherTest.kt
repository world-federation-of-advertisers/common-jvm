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

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfa.measurement.queue.testing.TestWork
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider

class PublisherTest {

  @Rule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private val projectId = "test-project"
  private val subscriptionIds =
    listOf("test-subscription-one", "test-subscription-two", "test-subscription-three")
  private val topicIds = listOf("test-topic-one", "test-topic-two", "test-topic-three")

  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  @Before
  fun setup() {
    runBlocking {
      emulatorClient =
        GooglePubSubEmulatorClient(
          host = pubSubEmulatorProvider.host,
          port = pubSubEmulatorProvider.port,
        )
      subscriptionIds.zip(topicIds).forEach { (subscriptionId, topicId) ->
        emulatorClient.createTopic(projectId, topicId)
        emulatorClient.createSubscription(projectId, subscriptionId, topicId)
      }
    }
  }

  @After
  fun tearDown() {
    runBlocking {
      subscriptionIds.zip(topicIds).forEach { (subscriptionId, topicId) ->
        emulatorClient.deleteTopic(projectId, topicId)
        emulatorClient.deleteSubscription(projectId, subscriptionId)
      }
    }
  }

  @Test
  fun `should publish against different topics`() {
    runBlocking {
      val messages = listOf("UserName1", "UserName2")
      val publisher = Publisher(projectId, emulatorClient)
      publisher.publishMessage(topicIds[0], createTestWork(messages[0]))
      publisher.publishMessage(topicIds[1], createTestWork(messages[1]))
      val subscriber = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)

      val firstMessageChannel =
        subscriber.subscribe<TestWork>(subscriptionIds[0], TestWork.parser())
      val firstMessage = firstMessageChannel.receive()
      firstMessage.ack()
      firstMessageChannel.cancel()
      assertThat(firstMessage.body.userName).isEqualTo(messages[0])

      val secondMessageChannel =
        subscriber.subscribe<TestWork>(subscriptionIds[1], TestWork.parser())
      val secondMessage = secondMessageChannel.receive()
      secondMessage.ack()
      secondMessageChannel.cancel()
      assertThat(secondMessage.body.userName).isEqualTo(messages[1])
    }
  }

  @Test
  fun `should raise an Exception when topic doesn't exist`() {
    runBlocking {
      val publisher = Publisher(projectId, emulatorClient)
      val exception =
        assertThrows(Exception::class.java) {
          runBlocking {
            publisher.publishMessage("test-topic-id", createTestWork("test-user-name"))
          }
        }
      assertThat(exception).hasMessageThat().contains("Impossible to publish the message.")
    }
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().setUserName(message).setUserAge("25").setUserCountry("US").build()
  }
}

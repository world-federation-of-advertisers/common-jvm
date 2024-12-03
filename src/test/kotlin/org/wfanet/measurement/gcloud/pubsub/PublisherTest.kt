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
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.wfa.measurement.queue.testing.TestWork
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class PublisherTest {

  private val emulatorClient =
    GooglePubSubEmulatorClient(
      host = pubSubEmulatorProvider.host,
      port = pubSubEmulatorProvider.port,
    )

  @Before
  fun createTopicAndSubscription() {
    runBlocking {
      SUBSCRIPTION_IDS.zip(TOPIC_IDS).forEach { (subscriptionId, topicId) ->
        emulatorClient.createTopic(PROJECT_ID, topicId)
        emulatorClient.createSubscription(PROJECT_ID, subscriptionId, topicId)
      }
    }
  }

  @After
  fun deleteTopicAndSubscription() {
    runBlocking {
      SUBSCRIPTION_IDS.zip(TOPIC_IDS).forEach { (subscriptionId, topicId) ->
        emulatorClient.deleteTopic(PROJECT_ID, topicId)
        emulatorClient.deleteSubscription(PROJECT_ID, subscriptionId)
      }
    }
  }

  @Test
  fun `should publish against different topics`() {
    runBlocking {
      val messages = listOf("UserName1", "UserName2")
      val publisher = Publisher<TestWork>(PROJECT_ID, emulatorClient)
      publisher.publishMessage(TOPIC_IDS[0], createTestWork(messages[0]))
      publisher.publishMessage(TOPIC_IDS[1], createTestWork(messages[1]))
      val subscriber = Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
      val firstMessageChannel =
        subscriber.subscribe<TestWork>(SUBSCRIPTION_IDS[0], TestWork.parser())
      val firstMessage = firstMessageChannel.receive()
      firstMessage.ack()
      firstMessageChannel.cancel()
      assertThat(firstMessage.body.userName).isEqualTo(messages[0])
      val secondMessageChannel =
        subscriber.subscribe<TestWork>(SUBSCRIPTION_IDS[1], TestWork.parser())
      val secondMessage = secondMessageChannel.receive()
      secondMessage.ack()
      secondMessageChannel.cancel()
      assertThat(secondMessage.body.userName).isEqualTo(messages[1])
    }
  }

  @Test
  fun `should raise a TopicNotFoundException when topic doesn't exist`() {
    runBlocking {
      val publisher = Publisher<TestWork>(PROJECT_ID, emulatorClient)

      assertFailsWith<TopicNotFoundException> {
        runBlocking {
          publisher.publishMessage("test-topic-id", createTestWork("test-user-name"))
        }
      }
    }
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().apply {
      userName = message
      userAge = "25"
      userCountry = "US"
    }.build()
  }

  companion object {

    @ClassRule
    @JvmField
    val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    private const val PROJECT_ID = "test-project"
    private val SUBSCRIPTION_IDS =
      listOf("test-subscription-one", "test-subscription-two", "test-subscription-three")
    private val TOPIC_IDS = listOf("test-topic-one", "test-topic-two", "test-topic-three")
  }

}

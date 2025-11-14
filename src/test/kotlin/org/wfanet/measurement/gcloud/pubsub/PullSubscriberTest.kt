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
import com.google.protobuf.StringValue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider

@RunWith(JUnit4::class)
class PullSubscriberTest {

  private lateinit var emulatorClient: GooglePubSubEmulatorClient
  private lateinit var subscriber: Subscriber

  @Before
  fun setUp() {
    emulatorClient =
      GooglePubSubEmulatorClient(
        host = pubSubEmulatorProvider.host,
        port = pubSubEmulatorProvider.port,
      )

    subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        blockingContext = Dispatchers.IO,
        ackDeadlineExtensionIntervalSeconds = 0,
        ackDeadlineExtensionSeconds = 0,
      )

    runBlocking {
      emulatorClient.createTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, TOPIC_ID)
    }
  }

  @After
  fun tearDown() {
    runBlocking {
      try {
        emulatorClient.deleteTopic(PROJECT_ID, TOPIC_ID)
      } catch (e: Exception) {
        // Ignore errors
      }
      try {
        emulatorClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
      } catch (e: Exception) {
        // Ignore errors
      }
    }
    subscriber.close()
    emulatorClient.close()
  }

  @Test
  fun `pullSubscriber receives and acknowledges message`() = runBlocking {
    // Publish a message
    val publisher = Publisher<StringValue>(PROJECT_ID, emulatorClient)
    val testMessage = StringValue.newBuilder().setValue("test-message-1").build()
    publisher.publishMessage(TOPIC_ID, testMessage)

    // Subscribe and receive
    val channel = subscriber.subscribe(SUBSCRIPTION_ID, StringValue.parser())

    withTimeout(10000) {
      var receivedMessage: StringValue? = null
      var receivedAckId: String? = null

      launch {
        channel.consumeEach { queueMessage ->
          receivedMessage = queueMessage.body
          receivedAckId = queueMessage.ackId
          queueMessage.ack()
          subscriber.close() // Stop after first message
        }
      }

      // Wait for message to be received
      while (receivedMessage == null) {
        delay(100)
      }

      assertThat(receivedMessage?.value).isEqualTo("test-message-1")
      assertThat(receivedAckId).isNotEmpty()
    }
  }

  @Test
  fun `pullSubscriber exposes ackId in QueueMessage`() = runBlocking {
    // Publish a message
    val publisher = Publisher<StringValue>(PROJECT_ID, emulatorClient)
    val testMessage = StringValue.newBuilder().setValue("test-ack-id").build()
    publisher.publishMessage(TOPIC_ID, testMessage)

    // Subscribe and verify ackId is exposed
    val channel = subscriber.subscribe(SUBSCRIPTION_ID, StringValue.parser())

    withTimeout(10000) {
      var capturedAckId: String? = null

      launch {
        channel.consumeEach { queueMessage ->
          capturedAckId = queueMessage.ackId
          queueMessage.ack()
          subscriber.close()
        }
      }

      while (capturedAckId == null) {
        delay(100)
      }

      // AckId should be a non-empty string
      assertThat(capturedAckId).isNotEmpty()
      assertThat(capturedAckId).isNotNull()
    }
  }

  @Test
  fun `nack makes message available for redelivery`() = runBlocking {
    // Publish a message
    val publisher = Publisher<StringValue>(PROJECT_ID, emulatorClient)
    val testMessage = StringValue.newBuilder().setValue("test-nack").build()
    publisher.publishMessage(TOPIC_ID, testMessage)

    // Subscribe and nack
    val channel = subscriber.subscribe(SUBSCRIPTION_ID, StringValue.parser())

    withTimeout(15000) {
      var firstReceive = true
      var secondReceived = false

      launch {
        channel.consumeEach { queueMessage ->
          if (firstReceive) {
            firstReceive = false
            queueMessage.nack() // Should make message available again
          } else {
            secondReceived = true
            queueMessage.ack()
            subscriber.close()
          }
        }
      }

      // Wait for second receive
      while (!secondReceived) {
        delay(100)
      }

      assertThat(secondReceived).isTrue()
    }
  }

  @Test
  fun `multiple messages can be processed`(): Unit = runBlocking {
    // Publish multiple messages
    val publisher = Publisher<StringValue>(PROJECT_ID, emulatorClient)
    val messages =
      listOf("message-1", "message-2", "message-3").map {
        StringValue.newBuilder().setValue(it).build()
      }
    messages.forEach { publisher.publishMessage(TOPIC_ID, it) }

    // Subscribe and collect
    val channel = subscriber.subscribe(SUBSCRIPTION_ID, StringValue.parser())
    val receivedMessages = mutableListOf<String>()

    withTimeout(15000) {
      launch {
        channel.consumeEach { queueMessage ->
          receivedMessages.add(queueMessage.body.value)
          queueMessage.ack()

          if (receivedMessages.size >= 3) {
            subscriber.close()
          }
        }
      }

      while (receivedMessages.size < 3) {
        delay(100)
      }

      assertThat(receivedMessages).containsExactly("message-1", "message-2", "message-3")
    }
  }

  companion object {
    @ClassRule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    private const val PROJECT_ID = "test-project"
    private const val TOPIC_ID = "test-topic"
    private const val SUBSCRIPTION_ID = "test-subscription"
  }
}

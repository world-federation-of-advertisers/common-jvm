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
package org.wfanet.measurement.queue.testing

import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import com.google.common.truth.Truth.assertThat
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.gcloud.pubsub.TopicNotFoundException
import org.wfa.measurement.queue.testing.TestWork
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.queue.QueuePublisher
import org.wfanet.measurement.queue.QueueSubscriber

abstract class AbstractQueueTest {

  private lateinit var queuePublisher: QueuePublisher<TestWork>
  private lateinit var queueSubscriber: QueueSubscriber

  protected abstract fun createPublisher(): QueuePublisher<TestWork>
  protected abstract fun createSubscriber(): QueueSubscriber

  abstract suspend fun createTopicAndSubscription()
  abstract suspend fun deleteTopicAndSubscription()

  @Before
  fun initPublisherAndSubscriber() {
    queuePublisher = createPublisher()
    queueSubscriber = createSubscriber()
  }


  @Test
  fun `should raise a TopicNotFoundException when topic doesn't exist`() {
    runBlocking {

      assertFailsWith<TopicNotFoundException> {
        runBlocking {
          queuePublisher.publishMessage("test-topic-id", createTestWork(1L))
        }
      }
    }
  }

  @Test
  fun `should publish against different topics`() {
    runBlocking {
      createTopicAndSubscription()
      val ids = listOf(1L, 2L)
      queuePublisher.publishMessage(TOPIC_IDS[0], createTestWork(ids[0]))
      queuePublisher.publishMessage(TOPIC_IDS[1], createTestWork(ids[1]))
      val firstMessageChannel =
        queueSubscriber.subscribe<TestWork>(SUBSCRIPTION_IDS[0], TestWork.parser())
      val firstMessage = firstMessageChannel.receive()
      firstMessage.ack()
      firstMessageChannel.cancel()
      assertThat(firstMessage.body.id).isEqualTo(ids[0])
      val secondMessageChannel =
        queueSubscriber.subscribe<TestWork>(SUBSCRIPTION_IDS[1], TestWork.parser())
      val secondMessage = secondMessageChannel.receive()
      secondMessage.ack()
      secondMessageChannel.cancel()
      assertThat(secondMessage.body.id).isEqualTo(ids[1])
      deleteTopicAndSubscription()
    }
  }

  @Test
  fun `should receive and ack message`() {

    runBlocking {
      createTopicAndSubscription()
      val ids = listOf(1L, 2L, 3L)
      val messageChannel = queueSubscriber.subscribe<TestWork>(SUBSCRIPTION_IDS[0], TestWork.parser())

      for (id in ids) {
        queuePublisher.publishMessage(TOPIC_IDS[0], createTestWork(id))
        val message = messageChannel.receive()
        assertThat(message.body.id).isEqualTo(id)
        message.ack()
      }
      deleteTopicAndSubscription()
    }
  }

  @Test
  fun `message should be published again after nack`() {

    runBlocking {
      createTopicAndSubscription()
      val id = 1L
      queuePublisher.publishMessage(TOPIC_IDS[0], createTestWork(id))
      val messageChannel = queueSubscriber.subscribe<TestWork>(SUBSCRIPTION_IDS[0], TestWork.parser())
      var message = messageChannel.receive()
      assertThat(message.body.id).isEqualTo(id)
      message.nack()
      message = messageChannel.receive()
      assertThat(message.body.id).isEqualTo(id)
      message.ack()
      deleteTopicAndSubscription()
    }
  }

  private fun createTestWork(testWorkId: Long): TestWork {
    return testWork {
      id = testWorkId
      userName = "User Name"
      userAge = "25"
      userCountry = "US"
    }
  }

  companion object {

    val TOPIC_IDS = listOf("test-topic-1", "test-topic-2", "test-topic-3")
    val SUBSCRIPTION_IDS = listOf("test-subscription-1", "test-subscription-2", "test-subscription-3")

  }

}

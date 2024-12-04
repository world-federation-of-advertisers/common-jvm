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

import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.queue.AbstractQueueTest
import org.wfanet.measurement.queue.QueuePublisher
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfa.measurement.queue.testing.TestWork

class GooglePubSubTest: AbstractQueueTest() {

  private val emulatorClient =
    GooglePubSubEmulatorClient(
      host = pubSubEmulatorProvider.host,
      port = pubSubEmulatorProvider.port,
    )

  override fun createPublisher(): QueuePublisher<TestWork> {
    return Publisher<TestWork>(PROJECT_ID, emulatorClient)
  }

  override fun createSubscriber(): QueueSubscriber {
    return Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
  }

  override suspend fun createTopicAndSubscription() {
    runBlocking {
      SUBSCRIPTION_IDS.zip(TOPIC_IDS).forEach { (subscriptionId, topicId) ->
        emulatorClient.createTopic(PROJECT_ID, topicId)
        emulatorClient.createSubscription(PROJECT_ID, subscriptionId, topicId)
      }
    }
  }

  override suspend fun deleteTopicAndSubscription() {
    runBlocking {
      SUBSCRIPTION_IDS.zip(TOPIC_IDS).forEach { (subscriptionId, topicId) ->
        emulatorClient.deleteTopic(PROJECT_ID, topicId)
        emulatorClient.deleteSubscription(PROJECT_ID, subscriptionId)
      }
    }
  }

  companion object {

    @ClassRule
    @JvmField
    val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    private const val PROJECT_ID = "test-project"

  }

}

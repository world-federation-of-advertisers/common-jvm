// Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.api.core.ApiFutures
import com.google.api.core.SettableApiFuture
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.cloud.pubsub.v1.Subscriber as GoogleSubscriber
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Empty
import com.google.protobuf.StringValue
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PullResponse
import com.google.pubsub.v1.ReceivedMessage
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.threeten.bp.Duration

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class)
class SubscriberTest {
  @Test
  fun `subscribe does not log cancellation when ack cancels deadline extension`() = runTest {
    val pullCallable = mock<UnaryCallable<PullRequest, PullResponse>>()
    val modifyAckDeadlineCallable = mock<UnaryCallable<ModifyAckDeadlineRequest, Empty>>()
    val acknowledgeCallable = mock<UnaryCallable<AcknowledgeRequest, Empty>>()
    val subscriptionAdminClient =
      mock<SubscriptionAdminClient> {
        on { pullCallable() } doReturn pullCallable
        on { modifyAckDeadlineCallable() } doReturn modifyAckDeadlineCallable
        on { acknowledgeCallable() } doReturn acknowledgeCallable
      }
    val extensionFuture = SettableApiFuture.create<Empty>()
    val pullResponse =
      PullResponse.newBuilder()
        .addReceivedMessages(
          ReceivedMessage.newBuilder()
            .setAckId(ACK_ID)
            .setMessage(
              PubsubMessage.newBuilder().setData(StringValue.of("message").toByteString())
            )
        )
        .build()
    whenever(pullCallable.futureCall(any()))
      .thenReturn(
        ApiFutures.immediateFuture(pullResponse),
        ApiFutures.immediateFuture(PullResponse.getDefaultInstance()),
      )
    whenever(modifyAckDeadlineCallable.futureCall(any())).thenReturn(extensionFuture)
    whenever(acknowledgeCallable.futureCall(any()))
      .thenReturn(ApiFutures.immediateFuture(Empty.getDefaultInstance()))

    val testDispatcher = StandardTestDispatcher(testScheduler)
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = FakeGooglePubSubClient(subscriptionAdminClient),
        pullIntervalMillis = 10_000,
        ackDeadlineExtensionIntervalSeconds = 1,
        ackDeadlineExtensionSeconds = 10,
        blockingContext = testDispatcher,
      )
    val logRecords = ConcurrentLinkedQueue<LogRecord>()
    val loggingHandler =
      object : Handler() {
        override fun publish(record: LogRecord) {
          logRecords.add(record)
        }

        override fun flush() {}

        override fun close() {}
      }
    val rootLogger = Logger.getLogger("")
    rootLogger.addHandler(loggingHandler)

    try {
      val channel = subscriber.subscribe(SUBSCRIPTION_ID, StringValue.parser())
      val receivedMessage = async { channel.receive() }
      runCurrent()
      val queueMessage = receivedMessage.await()

      advanceTimeBy(1_000)
      runCurrent()
      verify(modifyAckDeadlineCallable).futureCall(any())

      queueMessage.ack()
      runCurrent()

      assertThat(extensionFuture.isCancelled).isTrue()
      assertThat(
          logRecords.filter { it.level == Level.WARNING && it.thrown is CancellationException }
        )
        .isEmpty()
    } finally {
      extensionFuture.cancel(false)
      subscriber.close()
      runCurrent()
      rootLogger.removeHandler(loggingHandler)
    }
  }

  private class FakeGooglePubSubClient(
    private val delegateSubscriptionAdminClient: SubscriptionAdminClient
  ) : GooglePubSubClient() {
    override fun buildTopicAdminClient(): TopicAdminClient = error("Not used")

    override fun buildSubscriptionAdminClient(): SubscriptionAdminClient =
      delegateSubscriptionAdminClient

    override fun buildSubscriber(
      projectId: String,
      subscriptionId: String,
      ackExtensionPeriod: Duration,
      messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit,
    ): GoogleSubscriber = error("Not used")

    override fun buildPublisher(projectId: String, topicId: String): GooglePublisher =
      error("Not used")
  }

  companion object {
    private const val PROJECT_ID = "test-project"
    private const val SUBSCRIPTION_ID = "test-subscription"
    private const val ACK_ID = "test-ack-id"
  }
}

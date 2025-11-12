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

import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PullRequest
import java.time.Duration
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber

/**
 * A Google Pub/Sub subscriber that uses the Pull API to retrieve messages from a subscription.
 *
 * Unlike the streaming subscriber API, this implementation uses synchronous pulls which expose real
 * acknowledgment IDs from [ReceivedMessage]. This allows manual control over ack deadline
 * extensions via [QueueSubscriber.QueueMessage.extendAckDeadline].
 *
 * **Note**: This implementation uses the Pull API, not the streaming subscriber API.
 *
 * @param projectId The Google Cloud project ID.
 * @param googlePubSubClient The client interface for interacting with the Google Pub/Sub service.
 * @param maxMessages The maximum number of messages to pull in each request.
 * @param pullIntervalMillis The interval between pull requests in milliseconds.
 * @param blockingContext The coroutine context used for producing the channel.
 */
class Subscriber(
  private val projectId: String,
  private val googlePubSubClient: GooglePubSubClient,
  private val maxMessages: Int,
  private val pullIntervalMillis: Long,
  blockingContext: @BlockingExecutor CoroutineContext,
) : QueueSubscriber {

  private val scope = CoroutineScope(blockingContext)

  @OptIn(ExperimentalCoroutinesApi::class) // For `produce`.
  override fun <T : Message> subscribe(
    subscriptionId: String,
    parser: Parser<T>,
  ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> =
    scope.produce(capacity = Channel.RENDEZVOUS) {
      val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)

      logger.info("Starting Pull Subscriber for subscription: $subscriptionId")

      try {
        while (isActive) {
          try {
            // Build pull request
            val pullRequest =
              PullRequest.newBuilder()
                .setSubscription(subscriptionName)
                .setMaxMessages(maxMessages)
                .build()

            // Pull messages from Pub/Sub
            val pullResponse =
              googlePubSubClient
                .getSubscriptionAdminClient()
                .pullCallable()
                .futureCall(pullRequest)
                .await()

            val messageCount = pullResponse.receivedMessagesCount
            if (messageCount > 0) {
              logger.info("Pulled $messageCount message(s) from subscription: $subscriptionId")
            }

            // Process each received message
            for (receivedMessage in pullResponse.receivedMessagesList) {
              val ackId = receivedMessage.ackId
              val message = receivedMessage.message

              // Parse the message body
              val parsedMessage = parser.parseFrom(message.data.toByteArray())

              // Create consumer with ack ID
              val consumer =
                PullMessageConsumer(
                  projectId = projectId,
                  subscriptionId = subscriptionId,
                  ackId = ackId,
                  googlePubSubClient = googlePubSubClient,
                )

              // Create queue message with ack ID
              val queueMessage =
                QueueSubscriber.QueueMessage(
                  body = parsedMessage,
                  ackId = ackId,
                  consumer = consumer,
                )

              // Send to channel
              logger.info(
                "Sending message with ackId=$ackId to channel for subscription: $subscriptionId"
              )
              send(queueMessage)
              logger.info("Message with ackId=$ackId sent successfully to channel")
            }

            // If no messages received, wait before next pull
            if (pullResponse.receivedMessagesCount == 0) {
              delay(pullIntervalMillis)
            }
          } catch (e: Exception) {
            logger.info("Error pulling messages from subscription $subscriptionId: ${e.message}")
            delay(pullIntervalMillis)
          }
        }
      } finally {
        logger.info("Pull Subscriber stopped for subscription: $subscriptionId")
      }
    }

  override fun close() {
    scope.cancel()
  }

  /**
   * MessageConsumer implementation that uses the Pull API's acknowledgment operations.
   *
   * @param projectId The Google Cloud project ID.
   * @param subscriptionId The subscription ID.
   * @param ackId The acknowledgment ID for this message.
   * @param googlePubSubClient The client for Pub/Sub operations.
   */
  private class PullMessageConsumer(
    private val projectId: String,
    private val subscriptionId: String,
    private val ackId: String,
    private val googlePubSubClient: GooglePubSubClient,
  ) : MessageConsumer {

    override fun ack() {
      try {
        val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
        logger.info("Acknowledging message with ackId=$ackId for subscription: $subscriptionName")
        val request =
          AcknowledgeRequest.newBuilder().setSubscription(subscriptionName).addAckIds(ackId).build()

        googlePubSubClient
          .getSubscriptionAdminClient()
          .acknowledgeCallable()
          .futureCall(request)
          .get() // Blocking call

        logger.info("Successfully acknowledged message with ackId=$ackId")
      } catch (e: Exception) {
        logger.info(
          "Failed to acknowledge message $ackId for subscription $projectId/$subscriptionId: ${e.message}"
        )
        throw e
      }
    }

    override fun nack() {
      // Nack by setting deadline to 0, making message immediately available for redelivery
      runBlocking {
        googlePubSubClient.modifyAckDeadline(
          projectId = projectId,
          subscriptionId = subscriptionId,
          ackIds = listOf(ackId),
          ackDeadlineSeconds = 0,
        )
      }
    }

    override fun extendAckDeadline(duration: Duration) {
      val deadlineSeconds = duration.toSeconds().toInt()
      runBlocking {
        googlePubSubClient.modifyAckDeadline(
          projectId = projectId,
          subscriptionId = subscriptionId,
          ackIds = listOf(ackId),
          ackDeadlineSeconds = deadlineSeconds,
        )
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

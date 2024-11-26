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

//import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber
import org.threeten.bp.Duration

/**
 * A Google Pub/Sub client that subscribes to a Pub/Sub subscription and provides messages in a
 * coroutine-based channel.
 *
 * @param projectId The Google Cloud project ID.
 * @param subscriberStub The gRPC stub for interacting with the Pub/Sub service.
 * @param blockingContext The coroutine context used for blocking operations, defaulting to
 *   [Dispatchers.IO].
 */
class Subscriber(
  val projectId: String,
  val googlePubSubClient: GooglePubSubClient,
  override val blockingContext: CoroutineContext = Dispatchers.IO,
) : QueueSubscriber {

  private val scope = CoroutineScope(blockingContext)
  private var isActive = true

  /**
   * Subscribes to a Pub/Sub subscription and returns a [ReceiveChannel] to consume messages
   * asynchronously.
   *
   * @param subscriptionId The ID of the Pub/Sub subscription.
   * @param parser A Protobuf [Parser] to parse messages into the desired type.
   * @return A [ReceiveChannel] that emits [QueueSubscriber.QueueMessage] objects.
   */
  override fun <T : Message> subscribe(
    subscriptionId: String,
    parser: Parser<T>,
  ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> {
    val channel = Channel<QueueSubscriber.QueueMessage<T>>(Channel.RENDEZVOUS)

    val subscriber = googlePubSubClient.buildSubscriber(
      projectId = projectId,
      subscriptionId = subscriptionId,
      ackExtensionPeriod = Duration.ofHours(6),
    ) { message, consumer ->
      scope.launch {
        try {

          val parsedMessage = parser.parseFrom(message.data.toByteArray())
          val queueMessage = QueueSubscriber.QueueMessage(
            body = parsedMessage,
            deliveryTag = message.messageId.hashCode().toLong(),
            consumer = object : MessageConsumer {
              override fun ack() {
                try {
                  consumer.ack()
                } catch (e: Exception) {
                  logger.warning("Failed to ack message: ${e.message}")
                }
              }

              override fun nack() {
                try {
                  consumer.nack()
                } catch (e: Exception) {
                  logger.warning("Failed to nack message: ${e.message}")
                }
              }
            },
          )

          channel.send(queueMessage)
        } catch (e: Exception) {
          logger.warning("Error processing message: ${e.message}")
          consumer.nack()
        }
      }
    }

    subscriber.startAsync().awaitRunning()
    logger.info("Subscriber started for subscription: $subscriptionId")

    // Handle channel closure
    channel.invokeOnClose {
      subscriber.stopAsync()
      subscriber.awaitTerminated()
    }

    return channel
  }

//  override fun <T : Message> subscribe(
//    subscriptionId: String,
//    parser: Parser<T>,
//  ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> {
//    val channel = Channel<QueueSubscriber.QueueMessage<T>>(Channel.RENDEZVOUS)
//    val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId).toString()
//
//    scope.launch {
//      while (isActive) {
//        try {
//          val pullRequest =
//            PullRequest.newBuilder().setMaxMessages(1).setSubscription(subscriptionName).build()
//
//          val pullResponse = runBlocking { subscriberStub.pullCallable()?.call(pullRequest) }
//
//          val receivedMessage =
//            pullResponse?.takeIf { it.receivedMessagesCount > 0 }?.getReceivedMessages(0)
//
//          if (receivedMessage == null) {
//            delay(1000)
//            continue
//          }
//
//          val pubsubMessage = receivedMessage.message
//          val ackId = receivedMessage.ackId
//          val parsedMessage = parser.parseFrom(pubsubMessage.data.toByteArray())
//          val extensionJob = startDeadlineExtension(subscriptionName, ackId)
//
//          val consumer =
//            object : MessageConsumer {
//              override fun ack() {
//                scope.launch {
//                  try {
//                    val ackRequest =
//                      AcknowledgeRequest.newBuilder()
//                        .setSubscription(subscriptionName)
//                        .addAckIds(ackId)
//                        .build()
//                    subscriberStub.acknowledgeCallable()?.call(ackRequest)
//                  } catch (e: Exception) {
//                    logger.warning("Failed to ack message: ${e.message}")
//                  } finally {
//                    extensionJob.cancel()
//                  }
//                }
//              }
//
//              override fun nack() {
//                scope.launch {
//                  try {
//                    modifyAckDeadline(subscriptionName, ackId, 0)
//                  } catch (e: Exception) {
//                    logger.warning("Failed to nack message: ${e.message}")
//                  } finally {
//                    extensionJob.cancel()
//                  }
//                }
//              }
//            }
//
//          val queueMessage =
//            QueueSubscriber.QueueMessage(
//              body = parsedMessage,
//              deliveryTag = pubsubMessage.messageId.hashCode().toLong(),
//              consumer = consumer,
//            )
//
//          try {
//            channel.send(queueMessage)
//          } catch (e: Exception) {
//            logger.warning("Failed to send message to channel: ${e.message}")
//            consumer.nack()
//          } finally {
//            extensionJob.join()
//          }
//        } catch (e: Exception) {
//          logger.warning("Error while pulling or processing message: ${e.message}")
//          delay(1000)
//        }
//      }
//    }
//
//    channel.invokeOnClose { isActive = false }
//
//    return channel
//  }

  /**
   * Starts a coroutine that extends the acknowledgment deadline for a message at regular intervals.
   *
   * @param subscriptionName The name of the Pub/Sub subscription.
   * @param ackId The acknowledgment ID of the message.
   * @return A [Job] representing the deadline extension coroutine.
   */
//  private fun CoroutineScope.startDeadlineExtension(subscriptionName: String, ackId: String): Job {
//    return launch {
//      try {
//        while (isActive) {
//          try {
//            modifyAckDeadline(subscriptionName, ackId, ackExtensionIntervalSeconds.toInt())
//            delay(ackExtensionIntervalSeconds * 1000)
//          } catch (e: Exception) {
//            logger.warning("Failed to extend ack deadline: ${e.message}")
//            break
//          }
//        }
//      } catch (e: Exception) {
//        logger.warning("Deadline extension coroutine failed: ${e.message}")
//      }
//    }
//  }

  /**
   * Modifies the acknowledgment deadline for a message.
   *
   * @param subscriptionName The name of the Pub/Sub subscription.
   * @param ackId The acknowledgment ID of the message.
   * @param extensionIntervalSeconds The new deadline in seconds.
   */
//  private fun modifyAckDeadline(
//    subscriptionName: String,
//    ackId: String,
//    extensionIntervalSeconds: Int,
//  ) {
//    val modifyRequest =
//      ModifyAckDeadlineRequest.newBuilder()
//        .setSubscription(subscriptionName)
//        .addAckIds(ackId)
//        .setAckDeadlineSeconds(extensionIntervalSeconds)
//        .build()
//    runBlocking { subscriberStub.modifyAckDeadlineCallable()?.call(modifyRequest) }
//  }

  override fun close() {
    isActive = false
    scope.cancel()
//    try {
//      subscriberStub.close()
//    } catch (e: Exception) {
//      logger.warning("Error closing subscriber stub: ${e.message}")
//    }
  }

  companion object {
    private val logger = Logger.getLogger(Subscriber::class.java.name)
  }
}

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

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.pubsub.v1.PullRequest
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.wfanet.measurement.queue.QueueClient
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.api.gax.core.CredentialsProvider;
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking

/**
 * A Google Pub/Sub client that processes one message at a time and ensures proper acknowledgment.
 *
 * @property projectId The Google Cloud Project ID
 * @property subscriptionId The Google Pub/Sub subscription ID
 * @property blockingContext The CoroutineContext to use for blocking operations
 */
class GooglePubSubClient(
  private val projectId: String,
  private val subscriberStub: SubscriberStub,
  override val blockingContext: CoroutineContext = Dispatchers.IO,
  private val ackExtensionIntervalSeconds: Long = 60L
) : QueueClient {

  private val scope = CoroutineScope(blockingContext)
  private var isActive = true

  override fun <T : Message> subscribe(
    subscriptionId: String,
    parser: Parser<T>
  ): ReceiveChannel<QueueClient.QueueMessage<T>> {
    val channel = Channel<QueueClient.QueueMessage<T>>(Channel.RENDEZVOUS)
    val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId).toString()

    scope.launch {
      while (isActive) {
          try {
            val pullRequest = PullRequest.newBuilder()
              .setMaxMessages(1)
              .setSubscription(subscriptionName)
              .build()

            val pullResponse = runBlocking {
              subscriberStub?.pullCallable()?.call(pullRequest)
            }

            val receivedMessage = pullResponse?.takeIf { it.receivedMessagesCount > 0 }?.getReceivedMessages(0)

            if (receivedMessage == null) {
              delay(1000)
              continue
            }

            val pubsubMessage = receivedMessage.message
            val ackId = receivedMessage.ackId
            val parsedMessage = parser.parseFrom(pubsubMessage.data.toByteArray())
            val extensionJob = startDeadlineExtension(subscriptionName, ackId)

            val consumer = object : AckReplyConsumer {
              override fun ack() {
                scope.launch {
                  try {
                    val ackRequest = AcknowledgeRequest.newBuilder()
                      .setSubscription(subscriptionName)
                      .addAckIds(ackId)
                      .build()
                    subscriberStub?.acknowledgeCallable()?.call(ackRequest)
                  } catch (e: Exception) {
                    logger.warning("Failed to ack message: ${e.message}")
                  } finally {
                    extensionJob.cancel()
                  }
                }
              }

              override fun nack() {
                scope.launch {
                  try {
                    val modifyRequest = ModifyAckDeadlineRequest.newBuilder()
                      .setSubscription(subscriptionName)
                      .addAckIds(ackId)
                      .setAckDeadlineSeconds(0)
                      .build()
                    subscriberStub?.modifyAckDeadlineCallable()?.call(modifyRequest)
                  } catch (e: Exception) {
                    logger.warning("Failed to nack message: ${e.message}")
                  } finally {
                    extensionJob.cancel()
                  }
                }
              }
            }

            val queueMessage = QueueClient.QueueMessage(
              body = parsedMessage,
              deliveryTag = pubsubMessage.messageId.hashCode().toLong(),
              consumer = consumer
            )

            try {
              channel.send(queueMessage)
            } catch (e: Exception) {
              logger.warning("Failed to send message to channel: ${e.message}")
              consumer.nack()
            } finally {
              extensionJob.join()
            }

          } catch (e: Exception) {
            logger.warning("Error while pulling or processing message: ${e.message}")
            delay(1000)
          }
      }
    }

    channel.invokeOnClose {
      isActive = false
    }

    return channel
  }

  private fun CoroutineScope.startDeadlineExtension(
    subscriptionName: String,
    ackId: String
  ): Job {
    return launch {
      try {
        while (isActive) {
          try {
            val modifyRequest = ModifyAckDeadlineRequest.newBuilder()
              .setSubscription(subscriptionName)
              .addAckIds(ackId)
              .setAckDeadlineSeconds(ackExtensionIntervalSeconds.toInt())
              .build()
            runBlocking {
              subscriberStub?.modifyAckDeadlineCallable()?.call(modifyRequest)
            }
            delay(ackExtensionIntervalSeconds * 1000)
          } catch (e: Exception) {
            logger.warning("Failed to extend ack deadline: ${e.message}")
            break
          }
        }
      } catch (e: Exception) {
        logger.warning("Deadline extension coroutine failed: ${e.message}")
      }
    }
  }

  override fun close() {
    isActive = false
    scope.cancel()
    try {
      subscriberStub?.close()
    } catch (e: Exception) {
      logger.warning("Error closing subscriber stub: ${e.message}")
    }
  }

  companion object {
    private val logger = Logger.getLogger(GooglePubSubClient::class.java.name)
  }
}










//
//
//---------------------
//
//
//
//override fun <T : Message> subscribe(
//  subscriptionId: String,
//  parser: Parser<T>
//): ReceiveChannel<QueueClient.QueueMessage<T>> {
//  val channel = Channel<QueueClient.QueueMessage<T>>(Channel.RENDEZVOUS)
//  val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId).toString()
//
//  // Launch coroutine for continuous pull
//  scope.launch {
//    while (isActive) {
//      // Try to acquire the mutex - will wait here if a message is being processed
//      processingMutex.withLock {
//        try {
//          // Pull exactly one message
//          val pullRequest = PullRequest.newBuilder()
//            .setMaxMessages(1)
//            .setSubscription(subscriptionName)
//            .build()
//
//          val pullResponse = withContext(blockingContext) {
//            subscriberStub?.pullCallable()?.call(pullRequest)
//          }
//
//          if (pullResponse?.receivedMessagesCount == 0) {
//            delay(1000) // Wait a bit before next pull if no message
//            return@withLock
//          }
//
//          // Process the single received message
//          val receivedMessage = pullResponse?.getReceivedMessages(0)
//          if (receivedMessage != null) {
//            val pubsubMessage = receivedMessage.message
//            val ackId = receivedMessage.ackId
//
//            try {
//              val parsedMessage = parser.parseFrom(pubsubMessage.data.toByteArray())
//
//              // Create custom AckReplyConsumer that uses the pull API
//              val consumer = object : AckReplyConsumer {
//                override fun ack() {
//                  scope.launch(blockingContext) {
//                    try {
//                      val ackRequest = AcknowledgeRequest.newBuilder()
//                        .setSubscription(subscriptionName)
//                        .addAckIds(ackId)
//                        .build()
//                      subscriberStub?.acknowledgeCallable()?.call(ackRequest)
//                    } catch (e: Exception) {
//                      logger.warning("Failed to ack message: ${e.message}")
//                    } finally {
//                      processingMutex.unlock() // Release the lock after ack
//                    }
//                  }
//                }
//
//                override fun nack() {
//                  scope.launch(blockingContext) {
//                    try {
//                      val modifyRequest = ModifyAckDeadlineRequest.newBuilder()
//                        .setSubscription(subscriptionName)
//                        .addAckIds(ackId)
//                        .setAckDeadlineSeconds(0) // Immediate retry
//                        .build()
//                      subscriberStub?.modifyAckDeadlineCallable()?.call(modifyRequest)
//                    } catch (e: Exception) {
//                      logger.warning("Failed to nack message: ${e.message}")
//                    } finally {
//                      processingMutex.unlock() // Release the lock after nack
//                    }
//                  }
//                }
//              }
//
//              val queueMessage = QueueClient.QueueMessage(
//                body = parsedMessage,
//                deliveryTag = pubsubMessage.messageId.hashCode().toLong(),
//                consumer = consumer
//              )
//
//              // Start deadline extension coroutine
//              val extensionJob = startDeadlineExtension(subscriptionName, ackId)
//
//              try {
//                // Don't unlock the mutex here - it will be unlocked after ack/nack
//                channel.send(queueMessage)
//              } catch (e: Exception) {
//                logger.warning("Failed to send message to channel: ${e.message}")
//                consumer.nack()
//                extensionJob.cancel()
//              } finally {
//                extensionJob.join()
//              }
//            } catch (e: Exception) {
//              logger.warning("Error processing message: ${e.message}")
//              // Nack the message directly using the stub
//              val modifyRequest = ModifyAckDeadlineRequest.newBuilder()
//                .setSubscription(subscriptionName)
//                .addAckIds(ackId)
//                .setAckDeadlineSeconds(0)
//                .build()
//              subscriberStub?.modifyAckDeadlineCallable()?.call(modifyRequest)
//              processingMutex.unlock() // Release the lock if parsing fails
//            }
//          }
//        } catch (e: Exception) {
//          logger.warning("Error in pull loop: ${e.message}")
//          delay(1000) // Wait before retry
//          processingMutex.unlock() // Release the lock if there's an error
//        }
//      }
//    }

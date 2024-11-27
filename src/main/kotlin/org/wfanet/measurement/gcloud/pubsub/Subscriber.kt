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
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import org.threeten.bp.Duration
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber

/**
 * A Google Pub/Sub client that subscribes to a Pub/Sub subscription and provides messages in a
 * coroutine-based channel.
 *
 * @param projectId The Google Cloud project ID.
 * @param googlePubSubClient The client interface for interacting with the Google Pub/Sub service.
 * @param blockingContext The coroutine context used for blocking operations, defaulting to
 *   [Dispatchers.IO].
 */
class Subscriber(
  val projectId: String,
  val googlePubSubClient: GooglePubSubClient,
  val blockingContext: CoroutineContext = Dispatchers.IO,
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

    val subscriber =
      googlePubSubClient.buildSubscriber(
        projectId = projectId,
        subscriptionId = subscriptionId,
        ackExtensionPeriod = Duration.ofHours(6),
      ) { message, consumer ->
        scope.launch {
          try {

            val parsedMessage = parser.parseFrom(message.data.toByteArray())
            val queueMessage =
              QueueSubscriber.QueueMessage(
                body = parsedMessage,
                deliveryTag = message.messageId.hashCode().toLong(),
                consumer =
                  object : MessageConsumer {
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

    channel.invokeOnClose {
      subscriber.stopAsync()
      subscriber.awaitTerminated()
    }

    return channel
  }

  override fun close() {
    isActive = false
    scope.cancel()
  }

  companion object {
    private val logger = Logger.getLogger(Subscriber::class.java.name)
  }
}

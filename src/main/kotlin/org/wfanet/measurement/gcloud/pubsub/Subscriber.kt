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
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jetbrains.annotations.BlockingExecutor
import org.threeten.bp.Duration
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber

/**
 * A Google Pub/Sub client that subscribes to a Pub/Sub subscription and provides messages in a
 * coroutine-based channel.
 *
 * @param projectId The Google Cloud project ID.
 * @param googlePubSubClient The client interface for interacting with the Google Pub/Sub service.
 * @param context The coroutine context used for producing the channel.
 */
class Subscriber(
  private val projectId: String,
  private val googlePubSubClient: GooglePubSubClient = DefaultGooglePubSubClient(),
  blockingContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : QueueSubscriber {

  private val scope = CoroutineScope(blockingContext)

  @OptIn(ExperimentalCoroutinesApi::class) // For `produce`.
  override fun <T : Message> subscribe(
    subscriptionId: String,
    parser: Parser<T>,
  ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> =
    scope.produce(capacity = Channel.RENDEZVOUS) {
      val subscriber =
        googlePubSubClient.buildSubscriber(
          projectId = projectId,
          subscriptionId = subscriptionId,
          ackExtensionPeriod = Duration.ofHours(6),
        ) { message, consumer ->
          logger.info("~~~~~~~~~~~~~ RECEIVED MESSAGE")
          val parsedMessage = parser.parseFrom(message.data.toByteArray())
          val queueMessage =
            QueueSubscriber.QueueMessage(
              body = parsedMessage,
              consumer = PubSubMessageConsumer(consumer),
            )
          logger.info("~~~~~~~~~~~~~ SENDING MESSAGE")
          val result = trySendBlocking(queueMessage)
          logger.info("~~~~~~~~~~~~~ result: ${result}")
          if (result.isClosed) {
            throw ClosedSendChannelException("Channel is closed. Cannot send message.")
          }
        }

//      scope.launch {
//        while (true) {
//          try {
//            logger.info("~~~~~~~~~~~~~ checking FAILURE")
//            val err: Throwable = subscriber.failureCause()
//            logger.severe("❌ Pub/Sub subscriber failed: ${err}")
//            this@produce.channel.close(err)
//          } catch (e: Exception) {
//            logger.severe("~~~~ exception while checking failure: ${e}")
//          }
//          delay(Duration.ofSeconds(10).toMillis())
//        }
//      }

      subscriber.startAsync().awaitRunning()
      logger.info("-Subscriber started for subscription: $subscriptionId")

      awaitClose {
        subscriber.stopAsync()
        subscriber.awaitTerminated()
        logger.info("~~~~~~~~~~~~~~ CLOSING SUBSCRIPTION")
      }
    }

  override fun close() {
    scope.cancel()
  }

  private class PubSubMessageConsumer(private val consumer: AckReplyConsumer) : MessageConsumer {
    override fun ack() {
      consumer.ack()
    }

    override fun nack() {
      consumer.nack()
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

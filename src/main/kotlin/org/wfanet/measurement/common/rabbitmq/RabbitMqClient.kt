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

package org.wfanet.measurement.common.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.trySendBlocking
import org.jetbrains.annotations.BlockingExecutor
import java.util.logging.Logger

/**
 * A RabbitMQ client that provides messaging capabilities through a Kotlin coroutines-based
 * interface.
 *
 * @property host The hostname of the RabbitMQ server
 * @property port The port of the RabbitMQ server
 * @property username The username for authentication
 * @property password The password for authentication
 * @property blockingContext The CoroutineContext to use for blocking operations
 */
class RabbitMqClient(
  private val host: String,
  private val port: Int,
  private val username: String,
  private val password: String,
  private val blockingContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : QueueClient {

  private val deliveryScope = CoroutineScope(blockingContext)

  private val rabbitMqConnection: Lazy<Connection> = lazy {
    val factory =
      ConnectionFactory().apply {
        this.host = this@RabbitMqClient.host
        this.port = this@RabbitMqClient.port
        this.username = this@RabbitMqClient.username
        this.password = this@RabbitMqClient.password
      }
    factory.newConnection()
  }

  /**
   * Subscribes to the specified queue and returns a ReceiveChannel of QueueMessage objects.
   *
   * @param queueName The name of the queue to subscribe to
   * @return A ReceiveChannel that will receive QueueMessages
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `produce`
  override fun <T> subscribe(queueName: String): ReceiveChannel<QueueClient.QueueMessage<T>> {

    if (!rabbitMqConnection.value.isOpen) {
      throw IllegalStateException("RabbitMQ connection is closed")
    }

    val channel = rabbitMqConnection.value.createChannel()

    return deliveryScope.produce {
      var currentConsumerTag: String? = null
      channel.basicConsume(
        queueName,
        false,
        object : DefaultConsumer(channel) {
          override fun handleDelivery(
            consumerTag: String,
            envelope: Envelope,
            properties: AMQP.BasicProperties,
            body: ByteArray,
          ) {
            currentConsumerTag = consumerTag
            @Suppress("UNCHECKED_CAST")
            val message =
              QueueClient.QueueMessage(body = body as T, deliveryTag = envelope.deliveryTag, channel = channel)
            this@produce.trySendBlocking(message).onFailure { e ->
              logger.severe("Failed to send message: ${e?.message}")
              message.nack(requeue = true)
            }
          }
        },
      )

      channel.addShutdownListener {
        this.close()
      }

      awaitClose {
        try {
          currentConsumerTag?.let { tag -> channel.basicCancel(tag) }
          channel.close()
        } catch (e: Exception) {
          logger.warning("Error cleaning up channel for queue $queueName: ${e.message}")
        }
      }
    }
  }

  override fun close() {

    deliveryScope.cancel()

    if (rabbitMqConnection.isInitialized()) {
      try {
        rabbitMqConnection.value.close()
      } catch (e: Exception) {
        logger.warning("Error closing connection: ${e.message}")
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

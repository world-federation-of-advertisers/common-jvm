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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel as KChannel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.BlockingExecutor

/**
 * A RabbitMQ client that provides messaging capabilities through a Kotlin coroutines-based
 * interface.
 *
 * @property host The hostname of the RabbitMQ server
 * @property port The port of the RabbitMQ server
 * @property username The username for authentication
 * @property password The password for authentication
 * @property queueName The name of the queue to subscribe to
 * @property blockingContext The CoroutineContext to use for blocking operations
 */
class RabbitMqClient(
  private val host: String,
  private val port: Int,
  private val username: String,
  private val password: String,
  private val queueName: String,
  private val blockingContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : AutoCloseable {
  private val rabbitChannel: Channel
  private val connectionKey = ConnectionKey(host, port, username, password)
  private val connectionInfo = getOrCreateConnection(connectionKey)

  companion object {
    private val connections = ConcurrentHashMap<ConnectionKey, ConnectionInfo>()

    private data class ConnectionKey(
      val host: String,
      val port: Int,
      val username: String,
      val password: String,
    )

    private class ConnectionInfo(
      val connection: Connection,
      val referenceCount: AtomicInteger = AtomicInteger(0),
    )

    @Synchronized
    private fun getOrCreateConnection(key: ConnectionKey): ConnectionInfo {
      return connections
        .computeIfAbsent(key) {
          val factory =
            ConnectionFactory().apply {
              host = key.host
              port = key.port
              username = key.username
              password = key.password
            }
          ConnectionInfo(factory.newConnection())
        }
        .also { it.referenceCount.incrementAndGet() }
    }

    @Synchronized
    private fun releaseConnection(key: ConnectionKey) {
      connections[key]?.let { info ->
        if (info.referenceCount.decrementAndGet() == 0) {
          connections.remove(key)
          try {
            info.connection.close()
          } catch (e: Exception) {
            println("Error closing connection: ${e.message}")
          }
        }
      }
    }
  }

  init {
    try {
      rabbitChannel = connectionInfo.connection.createChannel()
    } catch (e: Exception) {
      releaseConnection(connectionKey)
      throw RuntimeException("Failed to create channel", e)
    }
  }

  data class QueueMessage<T>(
    val body: T,
    private val deliveryTag: Long,
    private val channel: Channel?,
  ) {
    fun ack() {
      channel?.basicAck(deliveryTag, false)
    }

    fun nack(requeue: Boolean = true) {
      channel?.basicNack(deliveryTag, false, requeue)
    }
  }

  /**
   * Subscribes to the configured queue and returns a ReceiveChannel of QueueMessage objects.
   *
   * @return A ReceiveChannel that will receive QueueMessages
   */
  suspend fun <T> subscribe(): ReceiveChannel<QueueMessage<T>> {
    if (!connectionInfo.connection.isOpen) {
      throw IllegalStateException("RabbitMQ connection is closed")
    }
    val forwardChannel = KChannel<QueueMessage<T>>()

    rabbitChannel.basicConsume(
      queueName,
      false,
      object : DefaultConsumer(rabbitChannel) {
        override fun handleDelivery(
          consumerTag: String,
          envelope: Envelope,
          properties: AMQP.BasicProperties,
          body: ByteArray,
        ) {
          @Suppress("UNCHECKED_CAST")
          val message =
            QueueMessage(
              body = body as T,
              deliveryTag = envelope.deliveryTag,
              channel = rabbitChannel,
            )
          runBlocking { forwardChannel.send(message) }
        }
      },
    )

    rabbitChannel.addShutdownListener { runBlocking { forwardChannel.close() } }

    return forwardChannel
  }

  override fun close() {
    try {
      rabbitChannel.close()
    } catch (e: Exception) {
      println("Error closing channel: ${e.message}")
    } finally {
      releaseConnection(connectionKey)
    }
  }

  /** Returns the current number of active connections for testing/monitoring purposes */
  fun getActiveConnectionCount(): Int = connections.size
}

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
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.Connection
import kotlin.coroutines.cancellation.CancellationException
import kotlin.system.exitProcess
import kotlinx.coroutines.runBlocking
import picocli.CommandLine


/**
 * An abstract class that provides RabbitMQ messaging capabilities through command line configuration.
 *
 * This class handles connection management and message consumption from a RabbitMQ queue.
 * Subclasses must implement [runWork] to define message processing logic.
 *
 * The client can be configured to:
 * - Process a single message and exit using [RabbitMqFlags.consumeSingleMessage]
 * - Process messages continuously until terminated
 *
 * @property rabbitMqFlags Configuration parameters for RabbitMQ connection and behavior
 */
@CommandLine.Command(
  name = "RabbitMQClient",
  description = ["Worker implementation for processing RabbitMQ messages"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
abstract class RabbitMQClient : Runnable {

  @CommandLine.Mixin
  lateinit var rabbitMqFlags: RabbitMqFlags
    private set

  private lateinit var connection: Connection
  private lateinit var channel: Channel

  /**
   * Abstract method to be implemented by subclasses to process RabbitMQ messages.
   *
   * @param message The message body as a ByteArray
   */
  protected abstract suspend fun runWork(message: ByteArray)

  /**
   * Initializes and runs the RabbitMQ client.
   * Sets up the connection and starts consuming messages from the configured queue.
   *
   * @throws Exception if there's an error during setup or message processing
   */
  override fun run() = runBlocking {
    try {
      setupRabbitMQ()
      subscribeAndWait()
    } catch (e: Exception) {
      println("Fatal error during worker execution: ${e.message}")
      throw e
    }
  }

  /**
   * Sets up the RabbitMQ connection and channel.
   * If [RabbitMqFlags.consumeSingleMessage] is true, sets QoS prefetch count to 1,
   * meaning RabbitMQ will only deliver one unacknowledged message at a time to this consumer.
   */
  private fun setupRabbitMQ() {
    val factory = ConnectionFactory().apply {
      host = rabbitMqFlags.rabbitHost
      port = rabbitMqFlags.rabbitPort
      username = rabbitMqFlags.rabbitUsername
      password = rabbitMqFlags.rabbitPassword
    }
    connection = factory.newConnection()
    channel = connection.createChannel().apply {
      if (rabbitMqFlags.consumeSingleMessage) {
        basicQos(1)
      }
    }
  }

  /**
   * Subscribes to the configured queue and processes messages.
   * For each message received:
   * 1. Launches a coroutine to process the message using [runWork]
   * 2. Acknowledges or negatively acknowledges based on processing result
   * 3. If [RabbitMqFlags.consumeSingleMessage] is true, exits after processing one message
   */
  private fun subscribeAndWait() {
    channel.basicConsume(rabbitMqFlags.rabbitQueueName, false, object : DefaultConsumer(channel) {
      override fun handleDelivery(
        consumerTag: String,
        envelope: Envelope,
        properties: AMQP.BasicProperties,
        body: ByteArray
      ) {
        runBlocking {
          try {
            runWork(body)
            channel.basicAck(envelope.deliveryTag, false)
            if (rabbitMqFlags.consumeSingleMessage) {
              cleanup()
            }
          } catch (e: CancellationException) {
            println("Work was cancelled: ${e.message}")
            channel.basicNack(envelope.deliveryTag, false, true)
            if (rabbitMqFlags.consumeSingleMessage) {
              cleanup()
            }
          } catch (e: Exception) {
            println("Error processing message: ${e.message}")
            channel.basicNack(envelope.deliveryTag, false, true)
            if (rabbitMqFlags.consumeSingleMessage) {
              cleanup()
            }
          }
        }
      }
    })
  }

  /**
   * Performs cleanup by closing the channel and connection, then exits the process.
   * Called automatically after processing a message if [RabbitMqFlags.consumeSingleMessage] is true.
   *
   * @throws Exception if there's an error during cleanup
   */
  private fun cleanup() {
    try {
      if (::channel.isInitialized) {
        try {
          channel.close()
        } catch (e: Exception) {
          println("Error closing channel: ${e.message}")
        }
      }

      if (::connection.isInitialized) {
        try {
          connection.close()
        } catch (e: Exception) {
          println("Error closing connection: ${e.message}")
        }
      }

      exitProcess(0)
    } catch (e: Exception) {
      println("Error during cleanup: ${e.message}")
      exitProcess(1)
    }
  }

  /**
   * Configuration flags for RabbitMQ connection and behavior.
   */
  class RabbitMqFlags {
    @CommandLine.Option(
      names = ["--rabbitmq-host"],
      description = ["Host name of the RabbitMQ server."],
      required = true
    )
    lateinit var rabbitHost: String
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-port"],
      description = ["Port of the RabbitMQ server."],
      required = true
    )
    var rabbitPort: Int = 5672
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-username"],
      description = ["Username to authenticate to the RabbitMQ server."],
      required = true
    )
    lateinit var rabbitUsername: String
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-password"],
      description = ["Password to authenticate to the RabbitMQ server."],
      required = true
    )
    lateinit var rabbitPassword: String
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-queue-name"],
      description = ["The queue name to subscribe."],
      required = true
    )
    lateinit var rabbitQueueName: String
      private set

    @CommandLine.Option(
      names = ["--consume-single-message"],
      description = ["If set, only consumes a single message then exits."],
      required = false
    )
    var consumeSingleMessage: Boolean = false
      private set

  }
}

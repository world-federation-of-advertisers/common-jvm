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

import com.google.common.truth.Truth.assertThat
import com.rabbitmq.client.AMQP.Queue.DeclareOk
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.Charset
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.Test

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class RabbitMqClientTest {
  private lateinit var connectionFactory: ConnectionFactory
  private lateinit var monitorChannel: Channel
  private val queueName = "test-queue-${System.currentTimeMillis()}"

  @Before
  fun setup() {
    connectionFactory =
      ConnectionFactory().apply {
        host = "localhost"
        port = 5672
        username = "guest"
        password = "guest"
      }

    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        channel.queueDeclare(queueName, true, false, false, null)
      }
    }

    val monitorConnection = connectionFactory.newConnection()
    monitorChannel = monitorConnection.createChannel()
  }

  @After
  fun cleanup() {
    try {
      monitorChannel.close()
    } catch (e: Exception) {
      println("Failed to close monitor channel: ${e.message}")
    }

    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        try {
          channel.queueDelete(queueName)
        } catch (e: Exception) {
          println("Failed to delete queue: ${e.message}")
        }
      }
    }
  }

  private fun getQueueInfo(): DeclareOk {
    return monitorChannel.queueDeclarePassive(queueName)
  }

  @Test
  fun testMessagesConsumption() = runBlocking {
    val client =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        queueName = queueName,
        blockingContext = UnconfinedTestDispatcher(),
      )

    val messages = listOf("Message1", "Message2", "Message3")
    val receivedMessages = mutableListOf<String>()
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        messages.forEach { msg -> channel.basicPublish("", queueName, null, msg.toByteArray()) }
      }
    }
    assertThat(getQueueInfo().messageCount).isEqualTo(3)
    client.use { rabbitmq ->
      withTimeout(5.seconds) {
        val messageChannel = rabbitmq.subscribe<ByteArray>()
        for (message in messageChannel) {
          receivedMessages.add(message.body.toString(Charset.defaultCharset()))
          message.ack()
          if (receivedMessages.size == messages.size) {
            break
          }
        }
      }
    }

    assertThat(receivedMessages).containsExactlyElementsIn(messages)
    assertThat(getQueueInfo().messageCount).isEqualTo(0)
  }

  @Test
  fun testMessageAcknowledgement() = runBlocking {
    val client =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        queueName = queueName,
        blockingContext = UnconfinedTestDispatcher(),
      )
    assertThat(getQueueInfo().messageCount).isEqualTo(0)
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        repeat(2) { i ->
          val message = "Message$i"
          channel.basicPublish("", queueName, null, message.toByteArray())
        }
      }
    }

    assertThat(getQueueInfo().messageCount).isEqualTo(2)
    assertThat(getQueueInfo().consumerCount).isEqualTo(0)

    var messageCount = 0

    val receivedMessages = mutableListOf<String>()
    client.use { rabbitmq ->
      withTimeout(5.seconds) {
        val messageChannel = rabbitmq.subscribe<ByteArray>()
        for (message in messageChannel) {
          val content = message.body.toString(Charset.defaultCharset())
          messageCount++

          when (content) {
            "Message0" -> {
              message.ack()
              receivedMessages.add(message.body.toString(Charset.defaultCharset()))
            }

            "Message1" -> {
              message.nack(requeue = true)
              receivedMessages.add(message.body.toString(Charset.defaultCharset()))
              break
            }
          }
        }
      }
    }

    val finalQueueInfo = getQueueInfo()
    assertThat(finalQueueInfo.messageCount).isEqualTo(1)
    assertThat(messageCount).isEqualTo(2)
  }

  @Test
  fun `test nack re-enqueue the message`() = runBlocking {
    val client =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        queueName = queueName,
        blockingContext = UnconfinedTestDispatcher(),
      )
    assertThat(getQueueInfo().messageCount).isEqualTo(0)
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        channel.basicPublish("", queueName, null, "Message1".toByteArray())
      }
    }

    assertThat(getQueueInfo().messageCount).isEqualTo(1)
    assertThat(getQueueInfo().consumerCount).isEqualTo(0)

    var messageCount = 0

    client.use { rabbitmq ->
      withTimeout(5.seconds) {
        val messageChannel = rabbitmq.subscribe<ByteArray>()
        for (message in messageChannel) {
          val content = message.body.toString(Charset.defaultCharset())
          when {
            messageCount == 0 -> {
              messageCount++
              assertThat("Message1").isEqualTo(content)
              message.nack(requeue = true)
            }
            messageCount == 1 -> {
              messageCount++
              assertThat("Message1").isEqualTo(content)
              message.ack()
              break
            }
          }
        }
      }
    }

    val finalQueueInfo = getQueueInfo()
    assertThat(finalQueueInfo.messageCount).isEqualTo(0)
    assertThat(messageCount).isEqualTo(2)
  }

  @Test
  fun testConnectionReuse() = runBlocking {
    val client1 =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        queueName = queueName,
        blockingContext = UnconfinedTestDispatcher(),
      )

    val client2 =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        queueName = queueName,
        blockingContext = UnconfinedTestDispatcher(),
      )

    assertThat(client1.getActiveConnectionCount()).isEqualTo(1)
    client1.close()
    assertThat(client2.getActiveConnectionCount()).isEqualTo(1)
    client2.close()
    assertThat(client2.getActiveConnectionCount()).isEqualTo(0)
  }

  @Test
  fun testConnectionCleanup() = runBlocking {
    val client =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        queueName = queueName,
        blockingContext = UnconfinedTestDispatcher(),
      )

    assertThat(client.getActiveConnectionCount()).isEqualTo(1)

    client.use { rabbitmq ->
      val job = launch { rabbitmq.subscribe<ByteArray>().consumeEach { message -> message.ack() } }
      job.cancelAndJoin()
    }

    assertThat(client.getActiveConnectionCount()).isEqualTo(0)
  }
}

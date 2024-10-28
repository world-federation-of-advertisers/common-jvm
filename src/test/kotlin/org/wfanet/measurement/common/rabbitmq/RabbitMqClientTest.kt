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

import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.Charset
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import com.google.common.truth.Truth.assertThat
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay
import picocli.CommandLine

private class TestRabbitMQClient : RabbitMQClient() {
  private val processedMessages = Collections.synchronizedList(mutableListOf<String>())
  private var messageProcessedLatch = CountDownLatch(1)

  override suspend fun runWork(message: ByteArray) {
    processedMessages.add(message.toString(Charset.defaultCharset()))
    messageProcessedLatch.countDown()
  }

  fun getProcessedMessages(): List<String> = processedMessages.toList()

  fun waitForMessageProcessing(): Boolean {
    val result = messageProcessedLatch.await(5, TimeUnit.SECONDS)
    if (result && !rabbitMqFlags.consumeSingleMessage) {
      messageProcessedLatch = CountDownLatch(1)
    }
    return result
  }

  override fun run() = runBlocking {
    try {
      super.run()
    } catch (e: SecurityException) {
      if (!e.message!!.contains("System.exit")) {
        throw e
      }
    }
  }
}

class RabbitMQClientTest {
  private var testClient: TestRabbitMQClient? = null
  private var connectionFactory: ConnectionFactory? = null
  private val queueName = "test-queue-${System.currentTimeMillis()}"
  private val testMessage = "Hello, RabbitMQ!"

  @Before
  fun setup() {
    connectionFactory = ConnectionFactory().apply {
      host = "localhost"
      port = 5672
      username = "guest"
      password = "guest"
    }

    // Test connection and create queue
    connectionFactory?.newConnection()?.use { connection ->
      connection.createChannel().use { channel ->
        channel.queueDeclare(queueName, false, false, true, null)
      }
    }

  }

  @After
  fun cleanup() {
    connectionFactory?.newConnection()?.use { connection ->
      connection.createChannel().use { channel ->
        try {
          channel.queueDelete(queueName)
        } catch (e: Exception) {
          println("Failed to delete queue: ${e.message}")
        }
      }
    }
  }

  @Test
  fun `test single message consumption`() = runBlocking {
    // Publish test message
    testClient = TestRabbitMQClient()
    val cmd = CommandLine(testClient)
    cmd.parseArgs(
      "--rabbitmq-host=localhost",
      "--rabbitmq-port=5672",
      "--rabbitmq-username=guest",
      "--rabbitmq-password=guest",
      "--rabbitmq-queue-name=$queueName",
      "--consume-single-message=true"
    )
    connectionFactory?.newConnection()?.use { connection ->
      connection.createChannel().use { channel ->
        channel.basicPublish(
          "",
          queueName,
          null,
          testMessage.toByteArray()
        )
      }
    }

    val clientThread = Thread {
      testClient?.run()
    }
    clientThread.start()

    assertThat(testClient?.waitForMessageProcessing())
      .isTrue()

    assertThat(testClient?.getProcessedMessages()?.size).isEqualTo(1)
    assertThat(testClient?.getProcessedMessages()?.first()).isEqualTo(testMessage)
  }

  @Test
  fun testMultipleMessagesWhenConsumeSingleMessageIsFalse() {
    val multiMessageClient = TestRabbitMQClient()
    val cmd = CommandLine(multiMessageClient)
    cmd.parseArgs(
      "--rabbitmq-host=localhost",
      "--rabbitmq-port=5672",
      "--rabbitmq-username=guest",
      "--rabbitmq-password=guest",
      "--rabbitmq-queue-name=$queueName",
      "--consume-single-message=false"
    )

    val messages = listOf("Message1", "Message2", "Message3")

    runBlocking {
      connectionFactory?.newConnection()?.use { connection ->
        connection.createChannel().use { channel ->
          messages.forEach { msg ->
            channel.basicPublish("", queueName, null, msg.toByteArray())
          }
        }
      }

      val clientThread = Thread {
        multiMessageClient.run()
      }
      clientThread.start()
      repeat(messages.size) {
        assertThat(multiMessageClient.waitForMessageProcessing()).isTrue()
      }
      assertThat(multiMessageClient.getProcessedMessages()).containsExactlyElementsIn(messages)
    }
  }

  @Test
  fun testMultipleMessagesWhenConsumeSingleMessageIsTrue() {
    val singleMessageClient = TestRabbitMQClient()
    val cmd = CommandLine(singleMessageClient)
    cmd.parseArgs(
      "--rabbitmq-host=localhost",
      "--rabbitmq-port=5672",
      "--rabbitmq-username=guest",
      "--rabbitmq-password=guest",
      "--rabbitmq-queue-name=$queueName",
      "--consume-single-message=true"
    )

    val messages = listOf("Message1", "Message2", "Message3")

    runBlocking {
      connectionFactory?.newConnection()?.use { connection ->
        connection.createChannel().use { channel ->
          messages.forEach { msg ->
            channel.basicPublish("", queueName, null, msg.toByteArray())
          }
        }
      }
      val clientThread = Thread {
        singleMessageClient.run()
      }
      clientThread.start()
      repeat(messages.size) {
        assertThat(singleMessageClient.waitForMessageProcessing()).isTrue()
      }
      assertThat(singleMessageClient.getProcessedMessages()).containsExactly("Message1")
    }
  }

}

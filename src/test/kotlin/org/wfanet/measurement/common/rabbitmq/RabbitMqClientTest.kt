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
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.lang.ProcessBuilder
import java.nio.file.Files
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.wfanet.measurement.common.getRuntimePath
import org.wfa.measurement.common.rabbitmq.TestWork

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class RabbitMqClientTest {
//  private lateinit var connectionFactory: ConnectionFactory
//  private lateinit var monitorChannel: Channel
//  private lateinit var rabbitMqProcess: Process
//  private val queueName = "test-queue-${System.currentTimeMillis()}"


  companion object {
    private lateinit var connectionFactory: ConnectionFactory
    private lateinit var monitorChannel: Channel
    private lateinit var rabbitMqProcess: Process
    private val queueName = "test-queue-${System.currentTimeMillis()}"
    @BeforeClass
    @JvmStatic
    fun setupClass() {

      val rabbitmqServerPath = getRuntimePath(Paths.get("rabbitmq", "sbin", "rabbitmq-server"))
      val baseDir = createTempDir("rabbitmq_test").apply { deleteOnExit() }
      val processBuilder = ProcessBuilder(rabbitmqServerPath.toString())
        .redirectErrorStream(true)

      processBuilder.environment().apply {
        put("RABBITMQ_LOG_BASE", "$baseDir/log")
        put("RABBITMQ_MNESIA_BASE", "$baseDir/mnesia")
        put("HOME", baseDir.absolutePath)
      }

      rabbitMqProcess = processBuilder.start()

      val startupTimeout = 30000L

      BufferedReader(InputStreamReader(rabbitMqProcess.inputStream)).use { reader ->
        val startTime = System.currentTimeMillis()
        while (reader.readLine()?.contains("completed with") != true &&
          System.currentTimeMillis() - startTime <= startupTimeout
        ) Unit
        require(System.currentTimeMillis() - startTime <= startupTimeout) { "RabbitMQ startup timeout" }
      }

      if (!rabbitMqProcess.isAlive) {
        println("Failed to start RabbitMQ server. Process is not alive.")
      } else {
        println("RabbitMQ server started successfully.")
      }
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

    @AfterClass
    @JvmStatic
    fun cleanupClass() {

      connectionFactory.newConnection().use { connection ->
        connection.createChannel().use { channel ->
          try {
            channel.queueDelete(queueName)
          } catch (e: Exception) {
            println("Failed to delete queue: ${e.message}")
          }
        }
      }
      try {
        monitorChannel.close()
        rabbitMqProcess.destroy()
      } catch (e: Exception) {
        println("Failed to close monitor channel: ${e.message}")
      }
    }
  }

  @Before
  fun setup() {
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        try {
          channel.queuePurge(queueName)
        } catch (e: Exception) {
          println("Failed to purge queue: ${e.message}")
        }
      }
    }
  }
  private fun getQueueInfo(): DeclareOk {
    return monitorChannel.queueDeclarePassive(queueName)
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder()
      .setUserName(message)
      .setUserAge("25")
      .setUserCountry("US")
      .build()
  }

  @Test
  fun testMessagesConsumption() = runBlocking {

    val client =
      RabbitMqClient(
        host = "localhost",
        port = 5672,
        username = "guest",
        password = "guest",
        blockingContext = UnconfinedTestDispatcher(),
      )

    val messages = listOf("UserName1", "UserName2", "UserName3")
    val receivedMessages = mutableListOf<String>()
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        messages.forEach { msg -> channel.basicPublish("", queueName, null, createTestWork(msg).toByteArray()) }
      }
    }

    assertThat(getQueueInfo().messageCount).isEqualTo(3)
    client.use { rabbitmq ->
      val messageChannel = rabbitmq.subscribe<TestWork>(queueName, TestWork.parser())
      for (message in messageChannel) {
        receivedMessages.add(message.body.userName)
        message.ack()
        if (receivedMessages.size == messages.size) {
          break
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
        blockingContext = UnconfinedTestDispatcher(),
      )
    assertThat(getQueueInfo().messageCount).isEqualTo(0)
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        repeat(2) { i ->
          val message = "UserName$i"
          channel.basicPublish("", queueName, null, createTestWork(message).toByteArray())
        }
      }
    }

    assertThat(getQueueInfo().messageCount).isEqualTo(2)
    assertThat(getQueueInfo().consumerCount).isEqualTo(0)

    var messageCount = 0

    val receivedMessages = mutableListOf<String>()
    client.use { rabbitmq ->
      val messageChannel = rabbitmq.subscribe<TestWork>(queueName, TestWork.parser())
      for (message in messageChannel) {
        val content = message.body.userName
        messageCount++

        when (content) {
          "UserName0" -> {
            message.ack()
            receivedMessages.add(message.body.userName)
          }

          "UserName1" -> {
            message.nack(requeue = true)
            receivedMessages.add(message.body.userName)
            break
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
        blockingContext = UnconfinedTestDispatcher(),
      )
    assertThat(getQueueInfo().messageCount).isEqualTo(0)
    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        channel.basicPublish("", queueName, null, createTestWork("UserName1").toByteArray())
      }
    }

    assertThat(getQueueInfo().messageCount).isEqualTo(1)
    assertThat(getQueueInfo().consumerCount).isEqualTo(0)

    var messageCount = 0

    client.use { rabbitmq ->
      val messageChannel = rabbitmq.subscribe<TestWork>(queueName, TestWork.parser())
      for (message in messageChannel) {
        val content = message.body.userName
        when {
          messageCount == 0 -> {
            messageCount++
            assertThat("UserName1").isEqualTo(content)
            message.nack(requeue = true)
          }
          messageCount == 1 -> {
            messageCount++
            assertThat("UserName1").isEqualTo(content)
            message.ack()
            break
          }
        }
      }
    }

    val finalQueueInfo = getQueueInfo()
    assertThat(finalQueueInfo.messageCount).isEqualTo(0)
    assertThat(messageCount).isEqualTo(2)
  }
}

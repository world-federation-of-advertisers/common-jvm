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
import java.io.InputStreamReader
import java.lang.ProcessBuilder
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.getRuntimePath

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class RabbitMqClientTest {
  private lateinit var connectionFactory: ConnectionFactory
  private lateinit var monitorChannel: Channel
  private lateinit var rabbitMqProcess: Process
  private val queueName = "test-queue-${System.currentTimeMillis()}"

  @Before
  fun setup() {

    val rabbitmqServerPath = getRuntimePath(Paths.get("rabbitmq", "sbin", "rabbitmq-server"))

    val rabbitmqHome = rabbitmqServerPath?.parent?.parent

    val processBuilder = ProcessBuilder(rabbitmqServerPath.toString())
      .redirectErrorStream(true)

    val mnesiaDirPath = createTempDir("rabbitmq-mnesia").absolutePath
    val logDirPath = createTempDir("rabbitmq-logs").absolutePath

    // Set all necessary environment variables
    processBuilder.environment().apply {
      put("RABBITMQ_HOME", rabbitmqHome.toString())
      put("RABBITMQ_ENABLED_PLUGINS_FILE", rabbitmqHome?.resolve("etc/rabbitmq/enabled_plugins").toString())
      put("RABBITMQ_CONFIG_FILE", rabbitmqHome?.resolve("etc/rabbitmq/rabbitmq").toString())
      put("RABBITMQ_MNESIA_BASE", createTempDir("rabbitmq-mnesia").absolutePath)
      put("RABBITMQ_LOG_BASE", createTempDir("rabbitmq-logs").absolutePath)
      // Ensure Erlang can find the RabbitMQ application
      put("RABBITMQ_SERVER_CODE_PATH", rabbitmqHome?.resolve("ebin").toString())

      put("RABBITMQ_SERVER_START_ARGS", "")
      put("RABBITMQ_NODENAME", "rabbit@localhost")
      put("RABBITMQ_NODE_IP_ADDRESS", "127.0.0.1")

      put("ERL_LIBS", rabbitmqHome?.resolve("lib").toString())
      // Add the plugins directory to Erlang path
      put("RABBITMQ_PLUGINS_DIR", rabbitmqHome?.resolve("plugins").toString())
      put("RABBITMQ_PLUGINS_EXPAND_DIR", "$mnesiaDirPath/plugins-expand")
      // Set other important RabbitMQ variables
      put("RABBITMQ_PID_FILE", "$mnesiaDirPath/rabbit@localhost.pid")
      put("RABBITMQ_ALLOW_INPUT", "false")
      // Ensure we're using the bundled Erlang runtime if available
      rabbitmqHome?.resolve("erts-*/bin")?.toString()?.let { ertsPath ->
        put("ERLANG_HOME", ertsPath)
      }
    }

    println("RabbitMQ directory contents:")
    val rabbitmqDir = rabbitmqHome?.toFile()
    rabbitmqDir?.listFiles()?.forEach { file ->
      println("  ${file.name}")
      if (file.name == "ebin") {
        file.listFiles()?.forEach { ebinFile ->
          println("    ebin/${ebinFile.name}")
        }
      }
    }

    rabbitMqProcess = processBuilder.start()

    // Add a proper startup check with timeout
    val startupTimeout = 30000L // 30 seconds
    val startTime = System.currentTimeMillis()

    BufferedReader(InputStreamReader(rabbitMqProcess.inputStream)).use { reader ->
      println("Starting RabbitMQ server from $rabbitmqServerPath")
      var line: String?
      while (reader.readLine().also { line = it } != null) {
        println("~~~~~~~~~~~~~~~~~~~~>> $line \n")
        if (line?.contains("completed with") == true) {
          println("~~~~~~~~~~~~~~~~`RabbitMQ server started successfully")
          break
        }
        if (System.currentTimeMillis() - startTime > startupTimeout) {
          throw IllegalStateException("~~~~~~~~~~~~~~~~```RabbitMQ server failed to start within timeout")
        }
      }
    }

    Thread.sleep(500)
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

  @After
  fun cleanup() {
    try {
      monitorChannel.close()
      rabbitMqProcess.destroy()
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
        val messageChannel = rabbitmq.subscribe<ByteArray>(queueName)
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
        val messageChannel = rabbitmq.subscribe<ByteArray>(queueName)
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
        val messageChannel = rabbitmq.subscribe<ByteArray>(queueName)
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
}

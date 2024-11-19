package org.wfanet.measurement.common.rabbitmq.testing

import com.google.protobuf.Message
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import org.wfanet.measurement.common.rabbitmq.QueueClient
import org.jetbrains.annotations.BlockingExecutor
import com.google.protobuf.Parser

/** In-memory [QueueClient]. */
@OptIn(ExperimentalCoroutinesApi::class) // For `produce`
class InMemoryQueueClient(
  override val blockingContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : QueueClient {

  private val scope = CoroutineScope(blockingContext)
  private val messageChannel = Channel<ByteArray>()

  override fun <T : Message> subscribe(
    queueName: String,
    parser: Parser<T>
  ): ReceiveChannel<QueueClient.QueueMessage<T>> {
    return scope.produce {
      for (body in messageChannel) {
        send(QueueClient.QueueMessage(parser.parseFrom(body), 0L, mockRabbitChannel()))
      }
    }
  }

  fun sendMessage(body: ByteArray) {
    messageChannel.trySend(body)
  }

  override fun close() {
    scope.cancel()
    messageChannel.close()
  }

}

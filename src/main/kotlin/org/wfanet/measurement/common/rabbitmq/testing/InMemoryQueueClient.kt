package org.wfanet.measurement.common.rabbitmq.testing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import org.wfanet.measurement.common.rabbitmq.QueueClient

/** In-memory [QueueClient]. */
@OptIn(ExperimentalCoroutinesApi::class) // For `produce`
class InMemoryQueueClient : QueueClient {
  private val scope = CoroutineScope(Dispatchers.Default)
  private val messageChannel = Channel<ByteArray>(Channel.UNLIMITED)

  override fun <T> subscribe(queueName: String): ReceiveChannel<QueueClient.QueueMessage<T>> {
    return scope.produce {
      for (body in messageChannel) {
        @Suppress("UNCHECKED_CAST")
        send(QueueClient.QueueMessage(body as T, 0L, mockRabbitChannel()))
      }
    }
  }

  fun sendMessage(body: ByteArray) {
    messageChannel.trySend(body)
  }

  override fun close() {
    messageChannel.close()
  }

}

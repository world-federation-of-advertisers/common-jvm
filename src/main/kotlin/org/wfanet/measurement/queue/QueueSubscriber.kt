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

package org.wfanet.measurement.queue

import com.google.protobuf.Message
import com.google.protobuf.Parser
import java.time.Duration
import kotlinx.coroutines.channels.ReceiveChannel

interface MessageConsumer {
  fun ack()

  fun nack()

  /**
   * Extends the acknowledgment deadline for this message.
   *
   * @param duration The duration by which to extend the deadline from now.
   */
  fun extendAckDeadline(duration: Duration)
}

/**
 * A Queue client that subscribes to a subscription and provides messages in a coroutine-based
 * channel.
 */
interface QueueSubscriber : AutoCloseable {

  /**
   * Subscribes to a subscription and returns a [ReceiveChannel] to consume messages asynchronously.
   *
   * @param subscriptionId The ID of the subscription.
   * @param parser A Protobuf [Parser] to parse messages into the desired type.
   * @return A [ReceiveChannel] that emits [QueueSubscriber.QueueMessage] objects.
   */
  fun <T : Message> subscribe(
    subscriptionId: String,
    parser: Parser<T>,
  ): ReceiveChannel<QueueMessage<T>>

  data class QueueMessage<T>(
    val body: T,
    val ackId: String,
    private val consumer: MessageConsumer,
  ) {
    fun ack() {
      consumer.ack()
    }

    fun nack() {
      consumer.nack()
    }

    fun extendAckDeadline(duration: Duration) {
      consumer.extendAckDeadline(duration)
    }
  }
}

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

/**
 * A class for publishing messages to a Queue.
 */
interface QueuePublisher<T: Message> : AutoCloseable {

  /**
   * Publishes a message to the specified topic.
   *
   * @param topicId The ID of the topic to publish the message to.
   * @param message The message to be published, where [T] is a type that extends [Message].
   * @throws [TopicNotFoundException] If the provided Topic ID does not exist.
   */
  suspend fun publishMessage(topicId: String, message: T)
}

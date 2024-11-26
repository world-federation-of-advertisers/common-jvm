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

package org.wfanet.measurement.gcloud.pubsub

import com.google.protobuf.ByteString
import org.threeten.bp.Duration
import com.google.pubsub.v1.PubsubMessage
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.Subscriber

interface GooglePubSubClient {

  fun buildSubscriber(projectId: String, subscriptionId: String, ackExtensionPeriod: Duration, messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit): Subscriber
  fun publishMessage(projectId: String, topicId: String, messageContent: ByteString)
  fun topicExists(projectId: String, topicId: String): Boolean

}

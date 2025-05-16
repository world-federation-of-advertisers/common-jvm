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

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.Subscriber as GoogleSubscriber
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PullResponse
import com.google.api.core.ApiService.Listener
import com.google.api.core.ApiService.State
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import java.util.logging.Logger
import org.threeten.bp.Duration

class DefaultGooglePubSubClient : GooglePubSubClient() {
  override fun buildTopicAdminClient(): TopicAdminClient {
    return TopicAdminClient.create(TopicAdminSettings.newBuilder().build())
  }

  override fun buildSubscriber(
    projectId: String,
    subscriptionId: String,
    ackExtensionPeriod: Duration,
    messageHandler: (PubsubMessage, AckReplyConsumer) -> Unit,
  ): GoogleSubscriber {

    logger.severe("~~~~~~~~ creating subscription: ${projectId}, ${subscriptionId}")
    val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
    logger.info("~~~~~~~~~~~ subscriptoin name: ${subscriptionName}")
    val messageReceiver = MessageReceiver { message, consumer -> messageHandler(message, consumer) }
    val subscriberBuilder =
      GoogleSubscriber.newBuilder(subscriptionName, messageReceiver)
        .setMaxAckExtensionPeriod(ackExtensionPeriod)

    val subscriber =  subscriberBuilder.build()

    subscriber.addListener(
      object : Listener() {
        override fun failed(from: State?, failure: Throwable) {
          logger.severe("~~~~~~~~~~~~~~ subscriber failed: ${from}, failure: ${failure}")
        }
      },
      MoreExecutors.directExecutor()
    )
    testSingleMessage()
    return subscriber
  }

  fun testSingleMessage() {
    try {
      logger.info("~~~~~~~~~ getting credentials")
      val credentials = GoogleCredentials.getApplicationDefault()
      logger.info("~~~~~~~~~ building settings")
      val subscriberStubSettings = SubscriberStubSettings.newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
        .build()
      logger.info("~~~~~~~~~ building subscriber")
      val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)
      try {
        logger.info("~~~~~~~~~ make request")
        val pullRequest = PullRequest.newBuilder()
          .setMaxMessages(1)
          .setSubscription("projects/halo-cmm-dev/subscriptions/requisition-fulfiller-subscription")
          .build()

        val pullResponse = subscriber.pullCallable().call(pullRequest)

        for (receivedMessage in pullResponse.receivedMessagesList) {
          val message = receivedMessage.message
          println("Received message: ${message.data.toStringUtf8()}")

          val ackRequest = AcknowledgeRequest.newBuilder()
            .setSubscription("projects/halo-cmm-dev/subscriptions/requisition-fulfiller-subscription")
            .addAckIds(receivedMessage.ackId)
            .build()

          subscriber.acknowledgeCallable().call(ackRequest)
        }
      } catch (e: Exception){
        logger.severe("~~~~~~~~~~~~~~~~~~~ error1 with single message: ${e}")
      }finally {
        subscriber.shutdownNow()
        subscriber.awaitTermination(1, java.util.concurrent.TimeUnit.MINUTES)
      }
    }catch (e: Exception ) {
      logger.severe("~~~~~~~~~~~~~~~~~~~ error with single message: ${e}")
    }

  }

  override fun buildPublisher(projectId: String, topicId: String): GooglePublisher {
    val topicName = TopicName.of(projectId, topicId)
    return GooglePublisher.newBuilder(topicName).build()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }

}

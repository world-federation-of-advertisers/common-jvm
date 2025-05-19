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

import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.cloud.pubsub.v1.Subscriber as GoogleSubscriber
import com.google.api.core.ApiService.Listener
import com.google.api.core.ApiService.State
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.stub.HttpJsonSubscriberStub
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.TopicName
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.TimeUnit
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
    testMetadata()
    testHttp()
    testSingleMessageNoGrpc()
    testSingleMessage()
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

    return subscriber
  }

  fun testMetadata(){
    try {
      val metadataUrl = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
      try {
        val conn = URL(metadataUrl).openConnection() as HttpURLConnection
        conn.setRequestProperty("Metadata-Flavor", "Google")
        conn.requestMethod = "GET"
        conn.connectTimeout = 2_000
        conn.readTimeout = 2_000

        val code = conn.responseCode
        val body = conn.inputStream.bufferedReader().readText()
        logger.info("METADATA SERVER → HTTP $code → $body")
      } catch (e: Exception) {
        logger.severe("Cannot reach metadata server: ${e.message} \n\n ${e}")
      }
    }catch (e: Exception){
      logger.severe("Cannot reach metadata server2: ${e.message} \n\n ${e}")
    }
  }

  fun testHttp() {
    try{
      val credentials = GoogleCredentials.getApplicationDefault()
      credentials.refreshIfExpired()
      val project = "halo-cmm-dev"
      val subName = "requisition-fulfiller-subscription"
      val httpRequest = HttpRequest.newBuilder()
        .uri(URI.create("https://pubsub.googleapis.com/v1/projects/$project/subscriptions/$subName:pull"))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer ${credentials.accessToken.tokenValue}")
        .POST(HttpRequest.BodyPublishers.ofString("""{"maxMessages":1}"""))
        .build()

      val resp = HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString())
      logger.info("REST pull → HTTP ${resp.statusCode()} → ${resp.body()}")
    }catch (e: Exception) {
      logger.severe ("~~~~~~~~~~~~ AAA: ${e}")
    }
  }

  fun testSingleMessageNoGrpc() {
      val logger = Logger.getLogger("PubSubTest")
      val subscriptionName = "projects/halo-cmm-dev/subscriptions/requisition-fulfiller-subscription"

      try {
        // Get default application credentials
        val credentials = GoogleCredentials.getApplicationDefault()

        // Build SubscriberStubSettings using HTTP/JSON transport
        val subscriberStubSettings = SubscriberStubSettings.newBuilder()
          .setTransportChannelProvider(
            SubscriberStubSettings.defaultHttpJsonTransportProviderBuilder().build()
          )
          .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
          .build()

        // Create the subscriber stub
        HttpJsonSubscriberStub.create(subscriberStubSettings).use { subscriber ->
          // Build a PullRequest
          val pullRequest = PullRequest.newBuilder()
            .setMaxMessages(1)
            .setSubscription(subscriptionName)
            .build()

          logger.info("~~~~~ Sending pull request to Pub/Sub (HTTP/JSON)")

          // Pull the message
          val pullResponse = subscriber.pullCallable().call(pullRequest)

          if (pullResponse.receivedMessagesList.isEmpty()) {
            logger.info("~~~~~No messages received.")
          } else {
            for (receivedMessage in pullResponse.receivedMessagesList) {
              val message = receivedMessage.message
              logger.info("Received message: ${message.messageId} -> ${message.data.toStringUtf8()}")

              // Acknowledge the message
              val ackRequest = AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAckIds(receivedMessage.ackId)
                .build()

              subscriber.acknowledgeCallable().call(ackRequest)
              logger.info("Acknowledged message: ${message.messageId}")
            }
          }

          // Clean up
          subscriber.shutdown()
          subscriber.awaitTermination(30, TimeUnit.SECONDS)
        }

      } catch (e: Exception) {
        logger.severe("Error while pulling message: ${e.message}")
        e.printStackTrace()
      }
  }

  fun testSingleMessage() {
    try {
      logger.info("~~~~~~~~~ getting credentials")
      val credentials = GoogleCredentials.getApplicationDefault()
      logger.info("~~~~~~~~~ building settings")
      try {
        credentials.refreshIfExpired()
        logger.info("Access token (first 20 chars)… ${credentials.accessToken.tokenValue.take(20)}…")
      }catch (e: Exception){
        logger.severe("~~~~~~~~~~~ error dumping credentials: ${e}")
      }
      val subscriberStubSettings = SubscriberStubSettings.newBuilder()
        .setTransportChannelProvider(
          SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
            .setMaxInboundMessageSize(20 * 1024 * 1024)
            .build())
        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
        .build()
      logger.info("~~~~~~~~~ building subscriber")
      logger.info("~~~~~~~~~ Stub endpoint: ${subscriberStubSettings.endpoint}")
      val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)
      try {
        logger.info("~~~~~~~~~ make request")
        val pullRequest = PullRequest.newBuilder()
          .setMaxMessages(1)
          .setSubscription("projects/halo-cmm-dev/subscriptions/requisition-fulfiller-subscription")
          .build()
        logger.info("~~~~~~~~~ make request2")
        val pullResponse = subscriber.pullCallable().call(pullRequest)
        logger.info("~~~~~~~~~ make request3")
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

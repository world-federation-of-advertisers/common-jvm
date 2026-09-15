/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.grpc

import com.google.common.truth.Truth.assertThat
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.health.v1.HealthCheckRequest
import io.grpc.health.v1.HealthCheckResponse
import io.grpc.health.v1.HealthGrpc
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcCleanupRule
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ServiceConfigTest {
  @get:Rule val grpcCleanup = GrpcCleanupRule()

  @Test
  fun `asMap includes retry throttling policy in default config`() {
    val serviceConfig = ProtobufServiceConfig.DEFAULT.asMap()

    assertThat(serviceConfig["retryThrottling"])
      .isEqualTo(mapOf("maxTokens" to 10.0, "tokenRatio" to 0.1))
  }

  @Test
  fun `default config suppresses retries after failure threshold`() {
    val attemptCount = AtomicInteger()
    val serverName = InProcessServerBuilder.generateName()
    val service =
      object : HealthGrpc.HealthImplBase() {
        override fun check(
          request: HealthCheckRequest,
          responseObserver: StreamObserver<HealthCheckResponse>,
        ) {
          attemptCount.incrementAndGet()
          val trailers = Metadata().apply { put(RETRY_PUSHBACK_KEY, "0") }
          responseObserver.onError(Status.UNAVAILABLE.asRuntimeException(trailers))
        }
      }
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(service)
        .build()
        .start()
    )
    val channel =
      grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName)
          .directExecutor()
          .enableRetry()
          .defaultServiceConfig(ProtobufServiceConfig.DEFAULT.asMap())
          .build()
      )
    val stub = HealthGrpc.newBlockingStub(channel)

    assertFailsWith<StatusRuntimeException> { stub.check(HealthCheckRequest.getDefaultInstance()) }
    assertThat(attemptCount.get()).isEqualTo(5)

    assertFailsWith<StatusRuntimeException> { stub.check(HealthCheckRequest.getDefaultInstance()) }
    assertThat(attemptCount.get()).isEqualTo(6)
  }

  companion object {
    private val RETRY_PUSHBACK_KEY: Metadata.Key<String> =
      Metadata.Key.of("grpc-retry-pushback-ms", Metadata.ASCII_STRING_MARSHALLER)
  }
}

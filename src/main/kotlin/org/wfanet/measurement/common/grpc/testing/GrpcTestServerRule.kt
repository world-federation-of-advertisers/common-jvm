// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc.testing

import io.grpc.BindableService
import io.grpc.Channel
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import java.util.concurrent.Executor
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.ErrorLoggingServerInterceptor
import org.wfanet.measurement.common.grpc.LoggingServerInterceptor

class GrpcTestServerRule(
  customServerName: String? = null,
  private val logAllRequests: Boolean = false,
  private val executor: Executor? = null,
  private val addServices: Builder.() -> Unit,
) : TestRule {
  class Builder(val channel: Channel, private val serverBuilder: InProcessServerBuilder) {
    fun addService(service: BindableService) {
      serverBuilder.addService(service)
    }
    fun addService(service: ServerServiceDefinition) {
      serverBuilder.addService(service)
    }
  }

  private val grpcCleanupRule: GrpcCleanupRule = GrpcCleanupRule()
  private val serverName = customServerName ?: InProcessServerBuilder.generateName()

  val channel: Channel =
    grpcCleanupRule.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())

  override fun apply(base: Statement, description: Description): Statement {
    val newStatement =
      object : Statement() {
        override fun evaluate() {
          val server: Server =
            InProcessServerBuilder.forName(serverName)
              .apply {
                if (executor == null) {
                  directExecutor()
                } else {
                  executor(executor)
                }

                if (logAllRequests) {
                  intercept(LoggingServerInterceptor)
                } else {
                  intercept(ErrorLoggingServerInterceptor)
                }

                Builder(channel, this).addServices()
              }
              .build()

          grpcCleanupRule.register(server.start())
          base.evaluate()
        }
      }

    return grpcCleanupRule.apply(newStatement, description)
  }
}

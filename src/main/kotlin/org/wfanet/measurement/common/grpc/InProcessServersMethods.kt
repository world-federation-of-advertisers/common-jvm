/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.grpc

import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.inprocess.InProcessServerBuilder
import java.util.concurrent.TimeUnit

/** Utility methods for creating and managing in-process gRPC servers. */
object InProcessServersMethods {
  /**
   * Starts an in-process gRPC server with the specified service.
   *
   * @param serverName the name of the in-process server
   * @param commonServerFlags common server configuration flags
   * @param service the gRPC service definition to add to the server
   * @return the started [Server] instance
   */
  fun startInProcessServerWithService(
    serverName: String,
    commonServerFlags: CommonServer.Flags,
    service: ServerServiceDefinition,
  ): Server {
    val server: Server =
      InProcessServerBuilder.forName(serverName)
        .apply {
          directExecutor()
          addService(service)
          if (commonServerFlags.debugVerboseGrpcLogging) {
            intercept(LoggingServerInterceptor)
          } else {
            intercept(ErrorLoggingServerInterceptor)
          }
        }
        .build()

    Runtime.getRuntime()
      .addShutdownHook(
        object : Thread() {
          override fun run() {
            server.shutdown()
            try {
              // Wait for in-flight RPCs to complete.
              server.awaitTermination(
                commonServerFlags.shutdownGracePeriodSeconds.toLong(),
                TimeUnit.SECONDS,
              )
            } catch (e: InterruptedException) {
              currentThread().interrupt()
            }
            server.shutdownNow()
          }
        }
      )

    return server.start()
  }
}

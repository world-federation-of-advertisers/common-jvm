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

package org.wfanet.measurement.common.grpc

import io.grpc.BindableService
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.HealthStatusManager
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import java.io.IOException
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.properties.Delegates
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.common.crypto.SigningCerts
import picocli.CommandLine

class CommonServer
private constructor(
  private val nameForLogging: String,
  port: Int,
  healthPort: Int,
  threadPoolSize: Int,
  verboseGrpcLogging: Boolean,
  services: Iterable<ServerServiceDefinition>,
  sslContext: SslContext?,
) {
  init {
    require(threadPoolSize > 1)
  }
  private val executor: ExecutorService =
    ThreadPoolExecutor(1, threadPoolSize, 60L, TimeUnit.SECONDS, LinkedBlockingQueue())
  private val healthStatusManager = HealthStatusManager()
  private val started = AtomicBoolean(false)

  val port: Int
    get() = server.port

  val healthPort: Int
    get() = healthServer.port

  /**
   * Internal [Server].
   *
   * Visible only for testing.
   */
  @get:VisibleForTesting
  internal val server: Server by lazy {
    logger.info { "$nameForLogging thread pool size: $threadPoolSize" }
    if (verboseGrpcLogging) {
      logger.info { "$nameForLogging verbose gRPC server logging enabled" }
    }

    NettyServerBuilder.forPort(port)
      .apply {
        executor(executor)
        if (sslContext != null) {
          sslContext(sslContext)
        }
        services.forEach { addService(it) }
        maxInboundMessageSize(20 * 1024 * 1024) // 20MiB
        if (verboseGrpcLogging) {
          intercept(LoggingServerInterceptor)
        } else {
          intercept(ErrorLoggingServerInterceptor)
        }
      }
      .build()
  }

  private val healthServer: Server by lazy {
    NettyServerBuilder.forPort(healthPort)
      .executor(executor)
      .apply { addService(healthStatusManager.healthService) }
      .build()
  }

  @Throws(IOException::class)
  @Synchronized
  fun start(): CommonServer {
    check(!started.get()) { "$nameForLogging already started" }
    server.start()
    server.services.forEach {
      healthStatusManager.setStatus(it.serviceDescriptor.name, ServingStatus.SERVING)
    }
    healthServer.start()

    logger.log(Level.INFO, "$nameForLogging started, listening on $port")
    Runtime.getRuntime()
      .addShutdownHook(
        object : Thread() {
          override fun run() {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** $nameForLogging shutting down...")
            this@CommonServer.shutdown()
            blockUntilShutdown()
            System.err.println("*** $nameForLogging shut down")
          }
        }
      )
    started.set(true)
    return this
  }

  fun shutdown() {
    if (!started.get()) {
      return
    }

    healthServer.shutdown()
    server.shutdown()
    executor.shutdown()
  }

  @Throws(InterruptedException::class)
  fun blockUntilShutdown() {
    if (!started.get()) {
      return
    }

    server.awaitTermination()
    healthServer.awaitTermination()
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
  }

  class Flags {
    @set:CommandLine.Option(
      names = ["--port", "-p"],
      description = ["TCP port for gRPC server."],
      defaultValue = "8443",
    )
    var port by Delegates.notNull<Int>()
      private set

    @CommandLine.Mixin
    lateinit var tlsFlags: TlsFlags
      private set

    @set:CommandLine.Option(
      names = ["--require-client-auth"],
      description = ["Require client auth"],
      defaultValue = "true"
    )
    var clientAuthRequired by Delegates.notNull<Boolean>()
      private set

    @set:CommandLine.Option(
      names = ["--health-port"],
      description = ["TCP port for the non-TLS health server."],
      defaultValue = "8080",
    )
    var healthPort by Delegates.notNull<Int>()
      private set

    @CommandLine.Option(
      names = ["--grpc-thread-pool-size"],
      description = ["Size of thread pool for gRPC server.", "Defaults to number of cores * 2."],
    )
    var threadPoolSize: Int = DEFAULT_THREAD_POOL_SIZE
      private set

    @set:CommandLine.Option(
      names = ["--debug-verbose-grpc-server-logging"],
      description = ["Debug mode: log ALL gRPC requests and responses"],
      defaultValue = "false"
    )
    var debugVerboseGrpcLogging by Delegates.notNull<Boolean>()
      private set
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    val DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2

    /** Constructs a [CommonServer] from parameters. */
    fun fromParameters(
      verboseGrpcLogging: Boolean,
      certs: SigningCerts?,
      clientAuth: ClientAuth,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>,
      port: Int = 0,
      healthPort: Int = 0,
      threadPoolSize: Int = DEFAULT_THREAD_POOL_SIZE
    ): CommonServer {
      return CommonServer(
        nameForLogging,
        port,
        healthPort,
        threadPoolSize,
        verboseGrpcLogging,
        services,
        certs?.toServerTlsContext(clientAuth),
      )
    }

    @JvmName("fromFlagsServiceDefinition")
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>
    ): CommonServer {
      return fromParameters(
        flags.debugVerboseGrpcLogging,
        flags.tlsFlags.signingCerts,
        if (flags.clientAuthRequired) ClientAuth.REQUIRE else ClientAuth.NONE,
        nameForLogging,
        services,
        flags.port,
        flags.healthPort,
        flags.threadPoolSize
      )
    }

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      vararg services: ServerServiceDefinition
    ): CommonServer = fromFlags(flags, nameForLogging, services.asIterable())

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<BindableService>
    ): CommonServer = fromFlags(flags, nameForLogging, services.map { it.bindService() })

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      vararg services: BindableService
    ): CommonServer = fromFlags(flags, nameForLogging, services.map { it.bindService() })
  }
}

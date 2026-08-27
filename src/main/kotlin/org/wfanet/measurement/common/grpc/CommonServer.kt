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
import io.grpc.protobuf.services.ProtoReflectionServiceV1
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import java.io.IOException
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.properties.Delegates
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.common.crypto.SigningCerts
import picocli.CommandLine

class CommonServer
private constructor(
  private val nameForLogging: String,
  port: Int,
  healthPort: Int,
  private val shutdownGracePeriodSeconds: Int,
  verboseGrpcLogging: Boolean,
  services: Iterable<ServerServiceDefinition>,
  sslContext: SslContext?,
  executor: Executor?,
) : AutoCloseable {
  private val healthStatusManager = HealthStatusManager()
  private val started = AtomicBoolean(false)

  val port: Int
    get() = server.port

  val healthPort: Int
    get() = healthServer.port

  private val server: Server by lazy {
    if (verboseGrpcLogging) {
      logger.info { "$nameForLogging verbose gRPC server logging enabled" }
    }

    NettyServerBuilder.forPort(port)
      .apply {
        directExecutor()
        if (sslContext != null) {
          sslContext(sslContext)
        }
        for (service in services) {
          addService(service)
        }
        addService(ProtoReflectionServiceV1.newInstance())
        if (verboseGrpcLogging) {
          intercept(LoggingServerInterceptor)
        } else {
          intercept(ErrorLoggingServerInterceptor)
        }
        if (executor != null) {
          // CloseOnceServerInterceptor must be added last (i.e. run first) so that
          // OverloadAwareServerInterceptor's direct call to ServerCall.close on rejection goes
          // through it, guarding against a second close from the coroutine's own completion
          // handling.
          intercept(OverloadAwareServerInterceptor(executor))
          intercept(CloseOnceServerInterceptor)
        }
      }
      .build()
  }

  private val healthServer: Server by lazy {
    NettyServerBuilder.forPort(healthPort)
      .apply {
        directExecutor()
        addService(healthStatusManager.healthService)
      }
      .build()
  }

  private val shutdownHook =
    object : Thread() {
      override fun run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** $nameForLogging shutting down...")

        shutdown()
        try {
          // Wait for in-flight RPCs to complete.
          server.awaitTermination(shutdownGracePeriodSeconds.toLong(), TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
          currentThread().interrupt()
        }
        shutdownNow()

        System.err.println("*** $nameForLogging shut down")
      }
    }

  @Synchronized
  @Throws(IOException::class)
  fun start(): CommonServer {
    check(!started.get()) { "$nameForLogging already started" }
    server.start()
    server.services.forEach {
      healthStatusManager.setStatus(it.serviceDescriptor.name, ServingStatus.SERVING)
    }
    healthServer.start()

    logger.log(Level.INFO, "$nameForLogging started, listening on $port")
    Runtime.getRuntime().addShutdownHook(shutdownHook)
    started.set(true)
    return this
  }

  fun shutdown() {
    if (!started.get()) {
      return
    }

    healthServer.shutdown()
    server.shutdown()
  }

  private fun shutdownNow() {
    if (!started.get()) {
      return
    }

    healthServer.shutdownNow()
    server.shutdownNow()
  }

  @Blocking
  @Throws(InterruptedException::class)
  fun blockUntilShutdown() {
    if (!started.get()) {
      return
    }

    server.awaitTermination()
    healthServer.awaitTermination()
  }

  private val terminated: Boolean
    get() = server.isTerminated && healthServer.isTerminated

  override fun close() {
    if (!started.get()) {
      return
    }

    Runtime.getRuntime().removeShutdownHook(shutdownHook)

    var interrupted = false
    shutdown()
    while (!terminated) {
      try {
        blockUntilShutdown()
      } catch (e: InterruptedException) {
        if (!interrupted) {
          shutdownNow()
          interrupted = true
        }
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt()
    }
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
      defaultValue = "true",
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

    @set:CommandLine.Option(
      names = ["--debug-verbose-grpc-server-logging"],
      description = ["Debug mode: log ALL gRPC requests and responses"],
      defaultValue = "false",
    )
    var debugVerboseGrpcLogging by Delegates.notNull<Boolean>()
      private set

    @set:CommandLine.Option(
      names = ["--shutdown-grace-period-seconds"],
      description = ["Duration of the \"grace period\" for graceful shutdown in seconds"],
      defaultValue = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS.toString(),
    )
    var shutdownGracePeriodSeconds by Delegates.notNull<Int>()
      private set
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    const val DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS = 25

    /**
     * Constructs a [CommonServer] from parameters.
     *
     * If [executor] is non-null, [OverloadAwareServerInterceptor] is installed globally,
     * *overriding* the per-RPC coroutine dispatcher for every service registered with this server
     * -- regardless of what [kotlin.coroutines.CoroutineContext] each service was individually
     * constructed with. [executor] must therefore be the same [Executor] every registered service
     * actually uses for its own coroutine dispatcher; passing a different one will silently
     * redirect all of their dispatching to it instead. In exchange, a rejection from it is surfaced
     * to clients as a clean status (`RESOURCE_EXHAUSTED`, `UNAVAILABLE`, or `INTERNAL`) rather than
     * left to hang until deadline. See [OverloadAwareServerInterceptor].
     */
    fun fromParameters(
      verboseGrpcLogging: Boolean,
      certs: SigningCerts?,
      clientAuth: ClientAuth,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>,
      port: Int = 0,
      healthPort: Int = 0,
      shutdownGracePeriodSeconds: Int = DEFAULT_SHUTDOWN_GRACE_PERIOD_SECONDS,
      executor: Executor? = null,
    ): CommonServer {
      return CommonServer(
        nameForLogging,
        port,
        healthPort,
        shutdownGracePeriodSeconds,
        verboseGrpcLogging,
        services,
        certs?.toServerTlsContext(clientAuth),
        executor,
      )
    }

    @JvmName("fromFlagsServiceDefinition")
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>,
      executor: Executor? = null,
    ): CommonServer {
      return fromParameters(
        flags.debugVerboseGrpcLogging,
        flags.tlsFlags.signingCerts,
        if (flags.clientAuthRequired) ClientAuth.REQUIRE else ClientAuth.OPTIONAL,
        nameForLogging,
        services,
        flags.port,
        flags.healthPort,
        flags.shutdownGracePeriodSeconds,
        executor,
      )
    }

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      vararg services: ServerServiceDefinition,
      executor: Executor? = null,
    ): CommonServer = fromFlags(flags, nameForLogging, services.asIterable(), executor)

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<BindableService>,
      executor: Executor? = null,
    ): CommonServer = fromFlags(flags, nameForLogging, services.map { it.bindService() }, executor)

    /** Constructs a [CommonServer] from command-line flags. */
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      vararg services: BindableService,
      executor: Executor? = null,
    ): CommonServer = fromFlags(flags, nameForLogging, services.map { it.bindService() }, executor)
  }
}

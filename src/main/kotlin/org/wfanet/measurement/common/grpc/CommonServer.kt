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
import io.grpc.services.HealthStatusManager
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.properties.Delegates
import org.wfanet.measurement.common.crypto.SigningCerts
import picocli.CommandLine

private const val DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES = 4194304

class CommonServer
private constructor(
  private val nameForLogging: String,
  private val port: Int,
  services: Iterable<ServerServiceDefinition>,
  sslContext: SslContext?,
  maxInboundMessageSizeBytes: Int = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES
) {
  private val healthStatusManager = HealthStatusManager()

  val server: Server by lazy {
    NettyServerBuilder.forPort(port)
      .apply {
        sslContext?.let { sslContext(it) }
        addService(healthStatusManager.healthService)
        services.forEach { addService(it) }
        maxInboundMessageSize(maxInboundMessageSizeBytes)
      }
      .build()
  }

  @Throws(IOException::class)
  fun start(): CommonServer {
    server.start()
    server.services.forEach {
      healthStatusManager.setStatus(it.serviceDescriptor.name, ServingStatus.SERVING)
    }

    logger.log(Level.INFO, "$nameForLogging started, listening on $port")
    Runtime.getRuntime()
      .addShutdownHook(
        object : Thread() {
          override fun run() {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** $nameForLogging shutting down...")
            this@CommonServer.stop()
            System.err.println("*** $nameForLogging shut down")
          }
        }
      )
    return this
  }

  private fun stop() {
    server.shutdown()
  }

  @Throws(InterruptedException::class)
  fun blockUntilShutdown() {
    server.awaitTermination()
  }

  class Flags {
    @set:CommandLine.Option(
      names = ["--port", "-p"],
      description = ["TCP port for gRPC server."],
      defaultValue = "8080"
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
      names = ["--debug-verbose-grpc-server-logging"],
      description = ["Debug mode: log ALL gRPC requests and responses"],
      defaultValue = "false"
    )
    var debugVerboseGrpcLogging by Delegates.notNull<Boolean>()
      private set

    @set:CommandLine.Option(
      names = ["--max-inbound-message-size-bytes"],
      description = ["Maximum size of inbound messages, in bytes"],
      defaultValue = "$DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES"
    )
    var maxInboundMessageSizeBytes by Delegates.notNull<Int>()
      private set
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    /** Constructs a [CommonServer] from parameters. */
    fun fromParameters(
      port: Int,
      verboseGrpcLogging: Boolean,
      certs: SigningCerts?,
      clientAuth: ClientAuth,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>,
      maxInboundMessageSize: Int = DEFAULT_MAX_INBOUND_MESSAGE_SIZE_BYTES
    ): CommonServer {
      return CommonServer(
        nameForLogging,
        port,
        services.run { if (verboseGrpcLogging) map { it.withVerboseLogging() } else this },
        certs?.toServerTlsContext(clientAuth),
        maxInboundMessageSize
      )
    }

    @JvmName("fromFlagsServiceDefinition")
    fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>
    ): CommonServer {
      val certs =
        SigningCerts.fromPemFiles(
          certificateFile = flags.tlsFlags.certFile,
          privateKeyFile = flags.tlsFlags.privateKeyFile,
          trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
        )

      return fromParameters(
        flags.port,
        flags.debugVerboseGrpcLogging,
        certs,
        if (flags.clientAuthRequired) ClientAuth.REQUIRE else ClientAuth.NONE,
        nameForLogging,
        services,
        flags.maxInboundMessageSizeBytes
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

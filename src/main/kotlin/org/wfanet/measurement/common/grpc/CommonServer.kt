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
import java.io.File
import java.io.IOException
import java.security.cert.X509Certificate
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.properties.Delegates
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyStore
import org.wfanet.measurement.common.crypto.readCertificateCollection
import picocli.CommandLine

class CommonServer
private constructor(
  private val nameForLogging: String,
  private val port: Int,
  signingCerts: SigningCerts,
  clientAuth: ClientAuth,
  services: Iterable<ServerServiceDefinition>,
) {
  private val healthStatusManager = HealthStatusManager()

  /** [X509Certificate]s trusted by this server. */
  val trustedCertificates: Collection<X509Certificate> = signingCerts.trustedCertificates

  val server: Server by lazy {
    NettyServerBuilder.forPort(port)
      .apply {
        sslContext(signingCerts.toServerTlsContext(clientAuth))
        addService(healthStatusManager.healthService)
        services.forEach { addService(it) }
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
      description = ["TCP port for gRPC server"],
      defaultValue = "8080"
    )
    var port by Delegates.notNull<Int>()
      private set

    @CommandLine.Option(
      names = ["--tls-signing-key-id"],
      description = ["ID (blob key) of the TLS signing key"],
      required = true,
    )
    lateinit var tlsSigningKeyId: String
      private set

    @CommandLine.Option(
      names = ["--trusted-cert-collection"],
      description =
        [
          "Collection of trusted X.509 certificates in PEM format",
          "Required if --require-client-auth is true",
        ],
    )
    var trustedCertCollectionPem: File? = null
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
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    /** Constructs a [CommonServer] from parameters. */
    fun fromParameters(
      port: Int,
      verboseGrpcLogging: Boolean,
      certs: SigningCerts,
      clientAuth: ClientAuth,
      nameForLogging: String,
      services: Iterable<ServerServiceDefinition>
    ): CommonServer {
      return CommonServer(
        nameForLogging,
        port,
        certs,
        clientAuth,
        services.run { if (verboseGrpcLogging) map { it.withVerboseLogging() } else this },
      )
    }

    @JvmName("fromFlagsServiceDefinition")
    suspend fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      signingKeyStore: SigningKeyStore,
      services: Iterable<ServerServiceDefinition>
    ): CommonServer {
      return fromParameters(
        flags.port,
        flags.debugVerboseGrpcLogging,
        SigningCerts.fromFlags(flags, signingKeyStore),
        if (flags.clientAuthRequired) ClientAuth.REQUIRE else ClientAuth.NONE,
        nameForLogging,
        services
      )
    }

    /** Constructs a [CommonServer] from command-line flags. */
    suspend fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      signingKeyStore: SigningKeyStore,
      vararg services: ServerServiceDefinition
    ): CommonServer = fromFlags(flags, nameForLogging, signingKeyStore, services.asIterable())

    /** Constructs a [CommonServer] from command-line flags. */
    suspend fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      signingKeyStore: SigningKeyStore,
      services: Iterable<BindableService>
    ): CommonServer =
      fromFlags(flags, nameForLogging, signingKeyStore, services.map { it.bindService() })

    /** Constructs a [CommonServer] from command-line flags. */
    suspend fun fromFlags(
      flags: Flags,
      nameForLogging: String,
      signingKeyStore: SigningKeyStore,
      vararg services: BindableService
    ): CommonServer =
      fromFlags(flags, nameForLogging, signingKeyStore, services.map { it.bindService() })
  }
}

private suspend fun SigningCerts.Companion.fromFlags(
  flags: CommonServer.Flags,
  signingKeyStore: SigningKeyStore,
): SigningCerts {
  val trustedCertCollectionPem = flags.trustedCertCollectionPem
  val trustedCerts: Collection<X509Certificate> =
    if (trustedCertCollectionPem == null) {
      listOf()
    } else {
      withContext(Dispatchers.IO) {
        @Suppress("BlockingMethodInNonBlockingContext") // In non-blocking context.
        readCertificateCollection(trustedCertCollectionPem)
      }
    }
  val privateKeyHandle: SigningKeyHandle =
    requireNotNull(signingKeyStore.read(flags.tlsSigningKeyId)) {
      "Signing key with ID ${flags.tlsSigningKeyId} not found in key store"
    }
  return SigningCerts(privateKeyHandle, trustedCerts)
}

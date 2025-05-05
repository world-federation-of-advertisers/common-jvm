// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import io.grpc.health.v1.HealthCheckResponse
import io.grpc.health.v1.HealthGrpc
import io.grpc.health.v1.HealthGrpc.HealthBlockingStub
import io.grpc.health.v1.healthCheckRequest
import io.grpc.protobuf.services.HealthStatusManager
import io.grpc.testing.GrpcCleanupRule
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.util.logging.Logger
import org.jetbrains.annotations.Blocking
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.SigningCerts

private const val ALGORITHM = "ec"
private const val CURVE = "prime256v1"
private const val SERVICE = "DummyService"
private const val HOSTNAME = "localhost"
private const val SUBJECT_ALT_NAME_EXT = "subjectAltName=DNS:$HOSTNAME,IP:127.0.0.1"

@RunWith(JUnit4::class)
class TransportSecurityTest {
  private val tempDir: File = temporaryFolder.root
  private val healthStatusManager = HealthStatusManager()

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = tempDir.resolve("server.pem"),
      privateKeyFile = tempDir.resolve("server.key"),
      trustedCertCollectionFile = tempDir.resolve("client-root.pem"),
    )

  private val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = tempDir.resolve("client.pem"),
      privateKeyFile = tempDir.resolve("client.key"),
      trustedCertCollectionFile = tempDir.resolve("server-root.pem"),
    )

  @get:Rule val grpcCleanup = GrpcCleanupRule()

  private fun startCommonServer(clientAuth: ClientAuth): CommonServer {
    val server =
      CommonServer.fromParameters(
          port = 0, // Bind to an unused port.
          healthPort = 0, // Bind to an unused port.
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = clientAuth,
          nameForLogging = "test",
          services = listOf(healthStatusManager.healthService.withVerboseLogging()),
        )
        .start()
    healthStatusManager.setStatus(SERVICE, HealthCheckResponse.ServingStatus.SERVING)
    return server
  }

  @Test
  fun `TLS server valid`() {
    startCommonServer(ClientAuth.NONE).use { server ->
      // Verify server using openssl s_client.
      runCommand(
        "openssl",
        "s_client",
        "-connect",
        "$HOSTNAME:${server.port}",
        "-verify_return_error",
        "-CAfile",
        "server-root.pem",
        "-tls1_3",
        "-verify_hostname",
        HOSTNAME,
      )
    }
  }

  @Test
  fun `mTLS server valid`() {
    startCommonServer(ClientAuth.REQUIRE).use { server ->
      // Verify server using openssl s_client.
      runCommand(
        "openssl",
        "s_client",
        "-connect",
        "$HOSTNAME:${server.port}",
        "-verify_return_error",
        "-cert",
        "client.pem",
        "-key",
        "client.key",
        "-CAfile",
        "server-root.pem",
        "-verify_hostname",
        HOSTNAME,
        "-tls1_3",
      )
    }
  }

  @Test
  fun `TLS RPC succeeds`() {
    val response: HealthCheckResponse =
      startCommonServer(ClientAuth.NONE).use { server ->
        val channel =
          grpcCleanup.register(
            buildTlsChannel("$HOSTNAME:${server.port}", clientCerts.trustedCertificates.values)
          )
        val client: HealthBlockingStub = HealthGrpc.newBlockingStub(channel)

        client.check(healthCheckRequest { service = SERVICE })
      }

    assertThat(response.status).isEqualTo(HealthCheckResponse.ServingStatus.SERVING)
  }

  @Test
  fun `mTLS RPC succeeds`() {
    val response: HealthCheckResponse =
      startCommonServer(ClientAuth.REQUIRE).use { server ->
        val channel =
          grpcCleanup.register(buildMutualTlsChannel("$HOSTNAME:${server.port}", clientCerts))
        val client: HealthBlockingStub = HealthGrpc.newBlockingStub(channel)

        client.check(healthCheckRequest { service = SERVICE })
      }

    assertThat(response.status).isEqualTo(HealthCheckResponse.ServingStatus.SERVING)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.canonicalName)

    @JvmField @ClassRule val temporaryFolder: TemporaryFolder = TemporaryFolder()

    /**
     * Generate test certs and keys in [temporaryFolder].
     *
     * TODO(@SanjayVas): Use certs generated by Bazel instead.
     */
    @JvmStatic
    @BeforeClass
    fun generateCerts() {
      val extFile = temporaryFolder.root.resolve("san.ext")
      extFile.writeText(SUBJECT_ALT_NAME_EXT)

      generateRootCert("server-root", "/O=Server CA/CN=ca.server.example.com")
      generateSigningRequest("server", "/O=Server/CN=server.example.com")
      signCertificate("server", "server-root")

      generateRootCert("client-root", "/O=Client CA/CN=ca.client.example.com")
      generateSigningRequest("client", "/O=Client/CN=client.example.com")
      signCertificate("client", "client-root")
    }

    private fun generateRootCert(name: String, subject: String) {
      runCommand(
        "openssl",
        "req",
        "-out",
        "$name.pem",
        "-new",
        "-newkey",
        ALGORITHM,
        "-pkeyopt",
        "ec_paramgen_curve:$CURVE",
        "-nodes",
        "-keyout",
        "$name.key",
        "-x509",
        "-addext",
        SUBJECT_ALT_NAME_EXT,
        "-days",
        "3650",
        "-subj",
        subject,
      )
    }

    private fun generateSigningRequest(name: String, subject: String) {
      runCommand(
        "openssl",
        "req",
        "-out",
        "$name.csr",
        "-new",
        "-newkey",
        ALGORITHM,
        "-pkeyopt",
        "ec_paramgen_curve:$CURVE",
        "-nodes",
        "-keyout",
        "$name.key",
        "-addext",
        SUBJECT_ALT_NAME_EXT,
        "-subj",
        subject,
      )
    }

    private fun signCertificate(name: String, rootCertName: String) {
      runCommand(
        "openssl",
        "x509",
        "-in",
        "$name.csr",
        "-out",
        "$name.pem",
        "-req",
        "-CA",
        "$rootCertName.pem",
        "-CAkey",
        "$rootCertName.key",
        "-CAcreateserial",
        "-extfile",
        "san.ext",
      )
    }

    @Blocking
    private fun runCommand(vararg command: String) {
      logger.info("Running `${command.joinToString(" ")}`")
      val process: Process =
        ProcessBuilder(*command).directory(temporaryFolder.root).redirectErrorStream(true).start()

      val exitCode: Int = process.waitFor()
      val output: String = process.inputStream.use { it.bufferedReader().readText() }
      if (exitCode == 0) {
        logger.info(output)
      } else {
        error("Command failed with code $exitCode. Output:\n$output")
      }
    }
  }
}

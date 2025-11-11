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

package org.wfanet.measurement.gcloud.gcs.testing

import com.google.cloud.NoCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import java.io.IOException
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import kotlin.concurrent.thread
import org.jetbrains.annotations.Blocking

/**
 * Wrapper for [Storage] emulator.
 *
 * See
 * https://github.com/googleapis/java-storage/blob/v2.59.0/google-cloud-storage/src/test/java/com/google/cloud/storage/it/runner/registry/TestBench.java
 */
class StorageEmulator(private val useGrpc: Boolean = false) : AutoCloseable {
  private val errOutput = StringBuilder()
  private var closed = AtomicBoolean(false)
  @Volatile private lateinit var process: Process
  private lateinit var readThread: Thread

  val started: Boolean
    get() = ::process.isInitialized

  @Synchronized
  @Blocking
  fun start(): Storage {
    check(!started) { "Already started" }
    check(!closed.get()) { "Already closed" }

    maybePullImage()

    // Allocate ports.
    val httpPort = ServerSocket(0).use { it.localPort }
    val grpcPort = ServerSocket(0).use { it.localPort }

    val url = "http://$HOSTNAME:$httpPort"
    val containerName = "storage-testbench-${UUID.randomUUID()}"
    logger.info { "Starting container $containerName" }
    process =
      ProcessBuilder(
          "docker",
          "run",
          "-i",
          "--rm",
          "--publish",
          "$httpPort:9000",
          "--publish",
          "$grpcPort:9090",
          "--name",
          containerName,
          IMAGE,
          "gunicorn",
          "--bind=0.0.0.0:9000",
          "--worker-class=sync",
          "--threads=10",
          "--access-logfile=-",
          "--keep-alive=0",
          "testbench:run()",
        )
        .start()

    // Message output by emulator which indicates that it is ready.
    val readyMessage = "Booting worker"

    val readyFuture = CompletableFuture<Unit>()

    readThread =
      thread(start = true, isDaemon = true) {
        process.errorStream.use { input ->
          input.bufferedReader().use { reader ->
            while (true) {
              val line =
                try {
                  reader.readLine()
                } catch (e: IOException) {
                  if (closed.get()) {
                    null
                  } else {
                    throw e
                  }
                } ?: break
              errOutput.appendLine(line)
              if (line.contains(readyMessage)) {
                readyFuture.complete(Unit)
              }
            }
          }
        }
        if (!readyFuture.isDone) {
          readyFuture.completeExceptionally(
            IOException("End of stream reached unexpectedly: $errOutput")
          )
        }
      }

    try {
      readyFuture.orTimeout(1, TimeUnit.MINUTES).join()
    } catch (e: TimeoutException) {
      throw IOException("Timed out waiting for server to start: $errOutput", e)
    }

    return if (useGrpc) {
        startGrpcServer(url)
        StorageOptions.grpc()
          .setHost("http://$HOSTNAME:$grpcPort")
          .setEnableGrpcClientMetrics(false)
          .setAttemptDirectPath(false)
      } else {
        StorageOptions.http().setHost(url)
      }
      .setProjectId("fake-project")
      .setCredentials(NoCredentials.getInstance())
      .build()
      .service
  }

  @Blocking
  @Synchronized
  override fun close() {
    if (!started) {
      closed.set(true)
      return
    }

    if (!closed.get()) {
      closed.set(true)
      readThread.interrupt()
      process.destroy()
      onExit().join()
    }
  }

  /** Returns a future for the termination of the server process. */
  @Synchronized
  fun onExit(): CompletableFuture<Unit> {
    return process.onExit().thenApply { process ->
      if (!closed.get()) {
        throw IOException("Server exited unexpectedly with code ${process.exitValue()}: $errOutput")
      }
    }
  }

  @Blocking
  private fun startGrpcServer(baseUrl: String) {
    val request = HttpRequest.newBuilder(URI.create("$baseUrl/start_grpc?port=9090")).build()
    val response: HttpResponse<String> =
      HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      throw IOException("${response.uri()} failed with ${response.statusCode()}")
    }
  }

  companion object {
    private const val HOSTNAME = "localhost"
    private const val IMAGE = "gcr.io/cloud-devrel-public-resources/storage-testbench:v0.56.0"
    private const val IMAGE_PULL_TIMEOUT_MINUTES = 2L
    private val imagePulled = AtomicBoolean()
    private val logger = Logger.getLogger(this::class.java.enclosingClass.name)

    @Synchronized
    @Blocking
    private fun maybePullImage() {
      if (imagePulled.get()) {
        return
      }

      logger.info { "Pulling image $IMAGE" }
      val temporaryDirectory = Files.createTempDirectory("docker-pull")
      val errFile = temporaryDirectory.resolve("pull-stderr").toFile()
      try {
        val pullProcess =
          ProcessBuilder().command("docker", "pull", IMAGE).redirectError(errFile).start()
        pullProcess.waitFor(IMAGE_PULL_TIMEOUT_MINUTES, TimeUnit.MINUTES)
        val exitValue = pullProcess.exitValue()
        if (exitValue != 0) {
          throw IOException("docker pull exited with $exitValue: \n${errFile.readText()}")
        }
      } catch (e: InterruptedException) {
        throw IOException("docker pull timed out: \n${errFile.readText()}", e)
      }

      imagePulled.set(true)
    }
  }
}

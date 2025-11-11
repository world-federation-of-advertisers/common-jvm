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

package org.wfanet.measurement.gcloud.spanner.testing

import java.io.IOException
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.wfanet.measurement.common.getRuntimePath

private const val EMULATOR_HOSTNAME = "localhost"

/**
 * Wrapper for Cloud Spanner Emulator binary.
 *
 * @param port TCP port that the emulator should listen on, or 0 to allocate a port automatically
 */
class SpannerEmulator(private val port: Int = 0) : AutoCloseable {
  @Volatile private lateinit var emulator: Process

  val started: Boolean
    get() = this::emulator.isInitialized

  @Volatile private var _emulatorHost: String? = null
  val emulatorHost: String
    get() {
      check(started) { "Emulator not started" }
      return _emulatorHost!!
    }

  private val closed = AtomicBoolean(false)
  private lateinit var readThread: Thread
  private val errOutput = StringBuilder()

  /**
   * Starts the emulator process if it has not already been started.
   *
   * This suspends until the emulator is ready.
   *
   * @returns the emulator host
   */
  @Synchronized
  fun start(): String {
    if (started) {
      return emulatorHost
    }
    // Open a socket on `port`. This should reduce the likelihood that the port
    // is in use. Additionally, this will allocate a port if `port` is 0.
    val localPort = ServerSocket(port).use { it.localPort }

    val emulatorHost = "$EMULATOR_HOSTNAME:$localPort"
    _emulatorHost = emulatorHost
    emulator = ProcessBuilder(emulatorPath.toString(), "--host_port=$emulatorHost").start()

    // Message output by simulator which indicates that it is ready.
    val readyMessage = "Server address: $emulatorHost"

    val readyFuture = CompletableFuture<Unit>()

    readThread =
      thread(start = true, isDaemon = true) {
        emulator.errorStream.use { input ->
          input.bufferedReader().use { reader ->
            while (true) {
              val line = reader.readLine() ?: break
              errOutput.appendLine(line)
              if (line.contains(readyMessage)) {
                readyFuture.complete(Unit)
              }
            }
          }
        }
        if (!readyFuture.isDone) {
          readyFuture.completeExceptionally(
            IOException(
              "Emulator exited unexpectedly with code ${emulator.exitValue()}: $errOutput"
            )
          )
        }
      }

    readyFuture.join()
    return emulatorHost
  }

  @Synchronized
  override fun close() {
    if (started && !closed.get()) {
      closed.set(true)
      readThread.interrupt()
      emulator.destroy()
      onExit().join()
    }
  }

  /** Returns a future for the termination of the emulator process. */
  fun onExit(): CompletableFuture<Unit> {
    synchronized(this) { check(started) { "Not started" } }
    return emulator.onExit().thenApply { process ->
      if (!closed.get()) {
        throw IOException(
          "Emulator exited unexpectedly with code ${process.exitValue()}: $errOutput"
        )
      }
    }
  }

  companion object {
    private val emulatorPath: Path

    init {
      val runfilesRelativePath = Paths.get("cloud_spanner_emulator", "emulator")
      val runtimePath = getRuntimePath(runfilesRelativePath)
      check(runtimePath != null && Files.exists(runtimePath)) {
        "$runfilesRelativePath not found in runfiles"
      }
      check(Files.isExecutable(runtimePath)) { "$runtimePath is not executable" }

      emulatorPath = runtimePath
    }

    fun buildJdbcConnectionString(
      emulatorHost: String,
      project: String,
      instance: String,
      database: String,
    ): String {
      return "jdbc:cloudspanner://$emulatorHost/projects/$project/instances/$instance/databases/" +
        "$database;usePlainText=true;autoConfigEmulator=true"
    }
  }
}

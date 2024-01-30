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

import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.getRuntimePath

private const val EMULATOR_HOSTNAME = "localhost"
private const val INVALID_HOST_MESSAGE =
  "emulator host must be of the form $EMULATOR_HOSTNAME:<port>"

/**
 * Wrapper for Cloud Spanner Emulator binary.
 *
 * @param port TCP port that the emulator should listen on, or 0 to allocate a port automatically
 * @param coroutineContext Context for operations that may block
 */
class SpannerEmulator(
  private val port: Int = 0,
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : AutoCloseable {
  private val startMutex = Mutex()
  @Volatile private lateinit var emulator: Process

  val started: Boolean
    get() = this::emulator.isInitialized

  @Volatile private var _emulatorHost: String? = null
  val emulatorHost: String
    get() {
      check(started) { "Emulator not started" }
      return _emulatorHost!!
    }

  /**
   * Starts the emulator process if it has not already been started.
   *
   * This suspends until the emulator is ready.
   *
   * @returns the emulator host
   */
  suspend fun start(): String {
    if (started) {
      return emulatorHost
    }

    return startMutex.withLock {
      // Double-checked locking.
      if (started) {
        return@withLock emulatorHost
      }

      return withContext(coroutineContext) {
        // Open a socket on `port`. This should reduce the likelihood that the port
        // is in use. Additionally, this will allocate a port if `port` is 0.
        val localPort = ServerSocket(port).use { it.localPort }

        val emulatorHost = "$EMULATOR_HOSTNAME:$localPort"
        _emulatorHost = emulatorHost
        emulator = ProcessBuilder(emulatorPath.toString(), "--host_port=$emulatorHost").start()

        // Message output by simulator which indicates that it is ready.
        val readyMessage = "Server address: $emulatorHost"

        emulator.errorStream.use { input ->
          input.bufferedReader().use { reader ->
            do {
              yield()
              check(emulator.isAlive) { "Emulator stopped unexpectedly" }
              val line = reader.readLine()
            } while (!line.contains(readyMessage))
          }
        }

        emulatorHost
      }
    }
  }

  override fun close() {
    if (started) {
      emulator.destroy()
    }
  }

  fun buildJdbcConnectionString(project: String, instance: String, database: String): String =
    buildJdbcConnectionString(emulatorHost, project, instance, database)

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

    fun withHost(emulatorHost: String): SpannerEmulator {
      val lazyMessage: () -> String = { INVALID_HOST_MESSAGE }

      val parts = emulatorHost.split(':', limit = 2)
      require(parts.size == 2 && parts[0] == EMULATOR_HOSTNAME, lazyMessage)
      val port = requireNotNull(parts[1].toIntOrNull(), lazyMessage)

      return SpannerEmulator(port)
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

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

package org.wfanet.measurement.gcloud.testing

import java.io.IOException
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.properties.Delegates
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.common.getRuntimePath

/**
 * Wrapper for a Cloud Function binary process. Exposes a port where the process can receive data.
 * This class is used for starting a process that invokes Google Cloud Functions for tests.
 *
 * @param javaBinaryPath the runfiles-relative path of the binary that runs the Cloud Run Invoker
 * @param classTarget the class name that the invoker will run. This must be in the class path of
 *   the binary that will be run.
 * @param coroutineContext the context under which the process will run
 */
class FunctionsFrameworkInvokerProcess(
  private val javaBinaryPath: Path,
  private val classTarget: String,
  private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : AutoCloseable {
  private val startMutex = Mutex()
  // Published only once the process reports ready, so a failed start leaves this null and the
  // instance reusable rather than handing out a port for a process that never served.
  @Volatile private var process: Process? = null
  private var localPort by Delegates.notNull<Int>()
  /*
   * Indicates whether the process has started.
   */
  val started: Boolean
    get() = process != null

  /*
   * Returns the port the process is listening on.
   */
  val port: Int
    get() {
      check(started) { "CloudFunction process not started" }
      return localPort
    }

  /**
   * Starts the CloudFunction process if it has not already been started.
   *
   * This suspends until the process is ready.
   *
   * @param env Environment variables to pass to the process
   * @return The HTTP port the process is listening on
   */
  suspend fun start(env: Map<String, String>): Int {
    if (started) {
      return port
    }
    return startMutex.withLock {
      // Double-checked locking.
      if (started) {
        return@withLock port
      }
      return withContext(coroutineContext) {

        // Open a socket on `port`. This should reduce the likelihood that the port
        // is in use. Additionally, this will allocate a port if `port` is 0.
        localPort = ServerSocket(0).use { it.localPort }
        val runtimePath = getRuntimePath(javaBinaryPath)
        check(runtimePath != null && Files.exists(runtimePath)) {
          "$javaBinaryPath not found in runfiles"
        }

        val processBuilder =
          ProcessBuilder(
              runtimePath.toString(),
              // Add HTTP port configuration
              "--port",
              localPort.toString(),
              "--target",
              classTarget,
            )
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectErrorStream(true)

        // Set environment variables
        processBuilder.environment().putAll(env)
        // Start the process
        val launched: Process = processBuilder.start()
        val readyPattern = "Serving function..."
        // Written by the output coroutine on Dispatchers.IO and polled below on another
        // thread, so it has to be safely published or the poll can miss the ready message
        // and time out after the function is already serving.
        val isReady = AtomicBoolean()
        CoroutineScope(Dispatchers.IO).launch {
          launched.inputStream.bufferedReader().use { reader ->
            var line: String?
            try {
              while (reader.readLine().also { line = it } != null) {
                if (line != null && line!!.contains(readyPattern)) {
                  isReady.set(true)
                }
                logger.info(line)
              }
            } catch (e: IOException) {
              logger.info(e.message)
            }
          }
        }

        // Wait for the ready message or timeout. A process that never reports ready is torn down
        // here: leaving it running would hold its port and, because `started` would be true, let a
        // later start() hand back that port without ever having seen the function serve.
        val timeout: Duration = 10.seconds
        val startTime = TimeSource.Monotonic.markNow()
        try {
          while (!isReady.get()) {
            yield()
            check(launched.isAlive) { "Google Cloud Function stopped unexpectedly" }
            if (startTime.elapsedNow() >= timeout) {
              throw IllegalStateException("Timeout waiting for Google Cloud Function to start")
            }
          }
        } catch (e: Throwable) {
          terminate(launched)
          throw e
        }
        process = launched
        localPort
      }
    }
  }

  /** Closes the process if it has been started. */
  override fun close() {
    val current: Process? = process
    if (current != null) {
      terminate(current)
    }
  }

  /** Stops [target], escalating to a forced kill if it does not exit promptly. */
  private fun terminate(target: Process) {
    target.destroy()
    try {
      if (!target.waitFor(5, TimeUnit.SECONDS)) {
        target.destroyForcibly()
      }
    } catch (e: InterruptedException) {
      target.destroyForcibly()
      Thread.currentThread().interrupt()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

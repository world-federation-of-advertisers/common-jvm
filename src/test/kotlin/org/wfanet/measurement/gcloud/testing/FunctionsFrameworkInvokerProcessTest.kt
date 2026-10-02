/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class FunctionsFrameworkInvokerProcessTest {
  @Test
  fun `start fails when the binary is not in runfiles`() = runBlocking {
    val invokerProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = Paths.get("wfa_common_jvm", "missing-test-server"),
        classTarget = "unused",
      )

    val exception = assertFailsWith<IllegalStateException> { invokerProcess.start(READY_ENV) }

    assertThat(exception).hasMessageThat().contains("not found in runfiles")
    assertThat(invokerProcess.started).isFalse()
  }

  @Test
  fun `start publishes the process when it reports ready`() = runBlocking {
    val invokerProcess = newInvokerProcess()
    try {
      val port: Int = invokerProcess.start(READY_ENV)

      assertThat(invokerProcess.started).isTrue()
      assertThat(invokerProcess.port).isEqualTo(port)
    } finally {
      invokerProcess.close()
    }
  }

  @Test
  fun `start cleans up a process that exits before reporting ready`() = runBlocking {
    val invokerProcess = newInvokerProcess()
    try {
      val exception =
        assertFailsWith<IllegalStateException> { invokerProcess.start(EXIT_BEFORE_READY_ENV) }

      assertThat(exception).hasMessageThat().contains("stopped unexpectedly")
      assertThat(invokerProcess.started).isFalse()

      invokerProcess.start(READY_ENV)
      assertThat(invokerProcess.started).isTrue()
    } finally {
      invokerProcess.close()
    }
  }

  @Test
  fun `start returns the existing port when the process is already started`() = runBlocking {
    val invokerProcess = newInvokerProcess()
    try {
      val port: Int = invokerProcess.start(READY_ENV)

      val repeatedPort: Int = invokerProcess.start(EXIT_BEFORE_READY_ENV)

      assertThat(repeatedPort).isEqualTo(port)
      assertThat(invokerProcess.started).isTrue()
    } finally {
      invokerProcess.close()
    }
  }

  @Test
  fun `start cleans up a process that times out before reporting ready`() = runBlocking {
    val invokerProcess =
      newInvokerProcess(startupTimeout = Duration.ZERO, terminationTimeout = 5.seconds)
    try {
      val exception = assertFailsWith<IllegalStateException> { invokerProcess.start(HANG_ENV) }

      assertThat(exception).hasMessageThat().contains("Timeout")
      assertThat(invokerProcess.started).isFalse()
    } finally {
      invokerProcess.close()
    }
  }

  @Test
  fun `close clears process state and permits restart`() = runBlocking {
    val invokerProcess = newInvokerProcess()
    try {
      invokerProcess.start(READY_ENV)

      invokerProcess.close()

      assertThat(invokerProcess.started).isFalse()
      assertFailsWith<IllegalStateException> { invokerProcess.port }

      val restartedPort: Int = invokerProcess.start(READY_ENV)
      assertThat(invokerProcess.port).isEqualTo(restartedPort)
    } finally {
      invokerProcess.close()
    }
  }

  @Test
  fun `close forcibly terminates a process that ignores graceful termination`() = runBlocking {
    val invokerProcess =
      newInvokerProcess(startupTimeout = 10.seconds, terminationTimeout = Duration.ZERO)
    try {
      invokerProcess.start(IGNORE_TERMINATION_ENV)

      invokerProcess.close()

      assertThat(invokerProcess.started).isFalse()
    } finally {
      invokerProcess.close()
    }
  }

  private fun newInvokerProcess(): FunctionsFrameworkInvokerProcess {
    return newInvokerProcess(startupTimeout = 10.seconds, terminationTimeout = 5.seconds)
  }

  private fun newInvokerProcess(
    startupTimeout: Duration,
    terminationTimeout: Duration,
  ): FunctionsFrameworkInvokerProcess {
    return FunctionsFrameworkInvokerProcess(
      javaBinaryPath = TEST_SERVER_PATH,
      classTarget = "unused",
      startupTimeout = startupTimeout,
      terminationTimeout = terminationTimeout,
    )
  }

  companion object {
    private val TEST_SERVER_PATH: Path =
      Paths.get(
        "wfa_common_jvm",
        "src",
        "test",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "gcloud",
        "testing",
        "function_process_test_server",
      )
    private val READY_ENV: Map<String, String> = emptyMap()
    private val EXIT_BEFORE_READY_ENV: Map<String, String> =
      mapOf("FUNCTION_PROCESS_TEST_MODE" to "exit")
    private val HANG_ENV: Map<String, String> = mapOf("FUNCTION_PROCESS_TEST_MODE" to "hang")
    private val IGNORE_TERMINATION_ENV: Map<String, String> =
      mapOf("FUNCTION_PROCESS_TEST_MODE" to "ignore_term")
  }
}

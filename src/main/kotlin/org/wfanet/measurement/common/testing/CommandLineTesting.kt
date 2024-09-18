// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.testing

import com.google.common.truth.FailureMetadata
import com.google.common.truth.IntegerSubject
import com.google.common.truth.StringSubject
import com.google.common.truth.Subject
import com.google.common.truth.Truth.assertAbout
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.security.Permission

/** Utilities for testing command line applications. */
object CommandLineTesting {
  data class CapturedOutput(val out: String, val err: String, val status: Int)

  /**
   * Executes [block], capturing its output.
   *
   * @param block a function which calls the `main` function of a command line application
   */
  inline fun capturingOutput(block: () -> Unit): CapturedOutput {
    val originalOut = System.out
    val originalErr = System.err
    val outStream = ByteArrayOutputStream()
    val errStream = ByteArrayOutputStream()
    System.setOut(PrintStream(outStream, true))
    System.setErr(PrintStream(errStream, true))

    var status = 0
    try {
      block()
    } catch (e: ExitInterceptingSecurityManager.ExitException) {
      status = e.status
    } finally {
      System.setErr(originalErr)
      System.setOut(originalOut)
    }

    return CapturedOutput(outStream.toString(), errStream.toString(), status)
  }

  /**
   * Executes [main], capturing its output.
   *
   * @param args arguments to the command line application
   * @param main the `main` function of a command line application
   */
  inline fun capturingOutput(args: Array<String>, main: (Array<String>) -> Unit): CapturedOutput {
    return capturingOutput { main(args) }
  }

  fun assertThat(actual: CapturedOutput): CapturedOutputSubject =
    assertAbout(CapturedOutputSubject.capturedOutputs()).that(actual)

  class CapturedOutputSubject
  private constructor(metadata: FailureMetadata, private val actual: CapturedOutput?) :
    Subject(metadata, actual) {
    fun status(): IntegerSubject {
      return check("status").that(actual?.status)
    }

    fun out(): StringSubject {
      return check("out").that(actual?.out)
    }

    fun err(): StringSubject {
      return check("err").that(actual?.err)
    }

    companion object {
      fun capturedOutputs() =
        Factory<CapturedOutputSubject, CapturedOutput> { metadata, actual ->
          CapturedOutputSubject(metadata, actual)
        }
    }
  }
}

/**
 * [SecurityManager] which throws [ExitException] on exit with non-zero status.
 *
 * To install this in a Bazel test, set the
 * `com.google.testing.junit.runner.shouldInstallTestSecurityManager` system property to `false`.
 * This can be done via the `jvm_flags` attribute.
 */
object ExitInterceptingSecurityManager : SecurityManager() {
  class ExitException(val status: Int) : RuntimeException()

  override fun checkPermission(perm: Permission?) {
    // Allow everything.
  }

  override fun checkExit(status: Int) {
    super.checkExit(status)

    if (status != 0) {
      // Prevent actual exit from happening.
      throw ExitException(status)
    }
  }
}

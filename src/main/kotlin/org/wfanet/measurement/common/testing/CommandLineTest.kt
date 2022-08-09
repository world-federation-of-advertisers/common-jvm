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

import com.google.common.truth.Truth
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.security.Permission
import kotlin.test.assertFailsWith

/** Run a command line given args. Returns a String of system output. */
fun runCommandLineTest(status: Int = 0, block: () -> Unit) = capturingSystemOut {
  assertExitsWith(status) { block() }
}

private fun capturingSystemOut(block: () -> Unit): String {
  val originalOut = System.out
  val outputStream = ByteArrayOutputStream()

  System.setOut(PrintStream(outputStream, true))
  try {
    block()
  } finally {
    System.setOut(originalOut)
  }

  return outputStream.toString()
}

private fun assertExitsWith(status: Int, block: () -> Unit) {
  val exception: ExitException = assertFailsWith {
    val originalSecurityManager: SecurityManager? = System.getSecurityManager()
    System.setSecurityManager(
      object : SecurityManager() {
        override fun checkPermission(perm: Permission?) {
          // Allow everything.
        }

        override fun checkExit(status: Int) {
          super.checkExit(status)
          throw ExitException(status)
        }
      }
    )

    try {
      block()
    } finally {
      System.setSecurityManager(originalSecurityManager)
    }
  }
  Truth.assertThat(exception.status).isEqualTo(status)
}

private class ExitException(val status: Int) : RuntimeException()

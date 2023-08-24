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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import picocli.CommandLine.Command
import picocli.CommandLine.Parameters

const val NAME = "World"
val ARGS = arrayOf(NAME)

@RunWith(JUnit4::class)
class CommandLineTestingTest {
  @Test
  fun `capturingOutput captures STDOUT`() {
    val output: CommandLineTesting.CapturedOutput =
      CommandLineTesting.capturingOutput(ARGS, HelloCommandLine::main)

    assertThat(output).out().contains("Hello $NAME")
  }

  @Test
  fun `capturingOutput captures STDERR`() {
    val output: CommandLineTesting.CapturedOutput =
      CommandLineTesting.capturingOutput(ARGS, HelloCommandLine::main)

    assertThat(output).err().contains("Goodbye $NAME")
  }

  @Test
  fun `capturingOutput captures status when SecurityManager installed`() {
    val output: CommandLineTesting.CapturedOutput =
      CommandLineTesting.capturingOutput(emptyArray(), HelloCommandLine::main)

    assertThat(output).status().isEqualTo(2)
  }

  @Test
  fun `main throws ExitException on non-zero status when SecurityManger installed`() {
    val exception =
      assertFailsWith<ExitInterceptingSecurityManager.ExitException> {
        HelloCommandLine.main(emptyArray())
      }

    assertThat(exception.status).isEqualTo(2)
  }

  companion object {
    init {
      // Install security manager to intercept system exit.
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }
  }
}

@Command(name = "hello")
private class HelloCommandLine : Runnable {
  @Parameters(index = "0") private lateinit var name: String

  override fun run() {
    println("Hello $name")
    System.err.println("Goodbye $name")
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(HelloCommandLine(), args)
  }
}

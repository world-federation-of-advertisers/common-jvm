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
import java.time.Duration
import kotlin.test.assertFails
import org.junit.Test
import org.wfanet.measurement.common.DurationFormat
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.testing.CommandLineTesting.assertExitsWith
import org.wfanet.measurement.common.testing.CommandLineTesting.capturingSystemOut
import picocli.CommandLine.Command
import picocli.CommandLine.Parameters

const val NAME = "World"
const val ZERO_DURATION = "PT0S"
val ARGS = arrayOf(NAME)

class CommandLineTestingTest {
  @Test
  fun `assertExitsWith does not raise execption when exit code is 0`() {
    assertExitsWith(0) { HelloCommandLine.main(ARGS) }
  }

  @Test
  fun `assertExitsWith raises execption when exit code is not 0`() {
    assertFails { assertExitsWith(0) { HelloCommandLine.main(arrayOf()) } }
  }

  @Test
  fun `capturingSystemOut() returns system output`() {
    val output = capturingSystemOut { HelloCommandLine.main(ARGS) }

    assertThat(output).contains("Hello $NAME")
  }

  @Test
  fun `default doesn't parse ISO-8601`() {
    val output = capturingSystemOut {
      commandLineMain(HelloCommandLine(), args = arrayOf(NAME, "P1DT3H5M12.99S"))
    }

    assertThat(output).isEqualTo("Hello $NAME. The duration is $ZERO_DURATION")
  }

  @Test
  fun `passing in format parses ISO-8601`() {
    val output = capturingSystemOut {
      commandLineMain(
        HelloCommandLine(),
        args = arrayOf(NAME, "P1DT3H5M12.99S"),
        DurationFormat.ISO_8601
      )
    }

    assertThat(output).isEqualTo("Hello $NAME. The duration is P1DT3H5M12.99S")
  }
}

@Command(name = "hello")
private class HelloCommandLine : Runnable {
  @Parameters(index = "0") private lateinit var name: String
  @Parameters(index = "1") private lateinit var duration: Duration

  override fun run() {
    println("Hello $name. The duration is $duration")
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(HelloCommandLine(), args)
  }
}

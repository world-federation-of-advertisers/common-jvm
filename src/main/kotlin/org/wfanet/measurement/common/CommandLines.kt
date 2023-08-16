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

package org.wfanet.measurement.common

import java.time.Duration
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.javaMethod
import kotlin.system.exitProcess
import picocli.CommandLine

/**
 * A `main` function for a [CommandLine] application.
 *
 * @param command a function annotated with [@Command][CommandLine.Command] to execute
 * @param args command-line arguments
 */
fun commandLineMain(command: KFunction<*>, args: Array<String>) {
  val status: Int = command.toCommandLine().execute(*args)
  if (status != 0) {
    exitProcess(status)
  }
}

/**
 * A `main` function for a [CommandLine] application.
 *
 * @param command a [Runnable] annotated with [@Command][CommandLine.Command] to execute
 * @param args command-line arguments
 */
fun commandLineMain(
  command: Runnable,
  args: Array<String>,
  format: DurationFormat = DurationFormat.HUMAN_READABLE
) {
  val status: Int = command.toCommandLine(format).execute(*args)
  if (status != 0) {
    exitProcess(status)
  }
}

private fun KFunction<*>.toCommandLine(): CommandLine {
  return CommandLine(javaMethod).apply { registerConverters() }
}

private fun Runnable.toCommandLine(
  format: DurationFormat = DurationFormat.HUMAN_READABLE
): CommandLine {
  return CommandLine(this).apply { registerConverters(format) }
}

private fun CommandLine.registerConverters(format: DurationFormat = DurationFormat.HUMAN_READABLE) {
  registerConverter(Duration::class.java) { it.toDuration(format) }
}

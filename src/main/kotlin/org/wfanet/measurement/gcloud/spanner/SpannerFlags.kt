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

package org.wfanet.measurement.gcloud.spanner

import java.time.Duration
import kotlin.properties.Delegates
import picocli.CommandLine

interface SpannerParams {
  val projectName: String
  val instanceName: String
  val databaseName: String
  val readyTimeout: Duration
  val asyncThreadPoolSize: Int
  val emulatorHost: String?

  val jdbcConnectionString: String
    get() {
      val databasePath = "projects/$projectName/instances/$instanceName/databases/$databaseName"
      return if (emulatorHost == null) {
        "jdbc:cloudspanner:/$databasePath"
      } else {
        "jdbc:cloudspanner://$emulatorHost/$databasePath;usePlainText=true;autoConfigEmulator=true"
      }
    }
}

/** Common command-line flags for connecting to a single Spanner database. */
class SpannerFlags : SpannerParams {
  @CommandLine.Option(
    names = ["--spanner-project"],
    description = ["Name of the Spanner project."],
    required = true,
  )
  override lateinit var projectName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-instance"],
    description = ["Name of the Spanner instance."],
    required = true,
  )
  override lateinit var instanceName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-database"],
    description = ["Name of the Spanner database."],
    required = true,
  )
  override lateinit var databaseName: String
    private set

  @CommandLine.Option(
    names = ["--spanner-ready-timeout"],
    description = ["How long to wait for Spanner to be ready."],
    defaultValue = "10s",
  )
  override lateinit var readyTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--spanner-emulator-host"],
    description = ["Host name and port of the spanner emulator."],
    required = false,
  )
  override var emulatorHost: String? = null
    private set

  @set:CommandLine.Option(
    names = ["--spanner-async-thread-pool-size"],
    description = ["Size of the thread pool for Spanner async operations."],
    defaultValue = "8",
  )
  override var asyncThreadPoolSize: Int by Delegates.notNull()
    private set
}

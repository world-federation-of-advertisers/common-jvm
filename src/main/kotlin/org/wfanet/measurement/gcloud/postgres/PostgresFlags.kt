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

package org.wfanet.measurement.gcloud.postgres

import java.time.Duration
import picocli.CommandLine

/** Common command-line flags for connecting to a single Postgres database. */
class PostgresFlags {
  @CommandLine.Option(
    names = ["--postgres-database"],
    description = ["Name of the Postgres database."],
    required = true,
  )
  lateinit var database: String
    private set

  @CommandLine.Option(
    names = ["--postgres-cloud-sql-connection-name"],
    description = ["Instance connection name of the Postgres database."],
    required = true,
  )
  lateinit var cloudSqlInstance: String
    private set

  @CommandLine.Option(
    names = ["--postgres-user"],
    description = ["User of the Postgres database."],
    required = true,
  )
  lateinit var user: String
    private set

  @CommandLine.Option(
    names = ["--statement-timeout"],
    description = ["Statement timeout duration."],
    required = false,
  )
  var statementTimeout: Duration = Duration.ofSeconds(120)
    private set

  @CommandLine.Option(
    names = ["--max-idle-time-minutes"],
    description =
      ["Maximum duration a connection can be idle before being closed."],
    required = false,
  )
  var maxIdleTime: Duration = Duration.ofMinutes(5)
    private set

  @CommandLine.Option(
    names = ["--max-connection-pool-size"],
    description = ["Maximum number of connections in pool."],
    required = false,
  )
  var maxPoolSize: Int = 16
    private set

  @CommandLine.Option(
    names = ["--acquire-retry"],
    description = ["Maximum number of retries when acquiring a connection from the pool."],
    required = false,
  )
  var acquireRetry: Int = 10
    private set
}

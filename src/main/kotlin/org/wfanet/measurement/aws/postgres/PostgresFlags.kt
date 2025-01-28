// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.aws.postgres

import kotlin.properties.Delegates
import org.wfanet.measurement.aws.common.RegionConverter
import picocli.CommandLine
import software.amazon.awssdk.regions.Region

/** Common command-line flags for connecting to a single Postgres database. */
class PostgresFlags {
  @CommandLine.Option(
    names = ["--postgres-host"],
    description = ["Host name of the Postgres database."],
    required = true,
  )
  lateinit var host: String
    private set

  @set:CommandLine.Option(
    names = ["--postgres-port"],
    description = ["Port of the Postgres database."],
    required = true,
  )
  var port: Int by Delegates.notNull()
    private set

  @CommandLine.Option(
    names = ["--postgres-credential-secret-name"],
    description =
      ["Name of the AWS Secrets Manager secret that stores the password of the Postgres database."],
    required = true,
  )
  lateinit var credentialSecretName: String
    private set

  @CommandLine.Option(
    names = ["--postgres-region"],
    description = ["AWS region that the postgres is located in."],
    converter = [RegionConverter::class],
    required = true,
  )
  lateinit var region: Region
    private set

  val jdbcConnectionString: String
    get() {
      return "jdbc:postgresql://$host:$port/"
    }
}

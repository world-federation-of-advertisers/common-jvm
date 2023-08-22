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

import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import org.wfanet.measurement.aws.postgres.PostgresCredentials.Companion.getPostgresCredentialsFromAwsSecretManager

object PostgresConnectionFactories {

  @JvmStatic
  fun buildConnectionFactory(flags: PostgresFlags): ConnectionFactory {
    val postgresCredentials =
      getPostgresCredentialsFromAwsSecretManager(flags.region, flags.credentialSecretName)
    return ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
        .option(
          ConnectionFactoryOptions.DRIVER,
          PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER
        )
        .option(ConnectionFactoryOptions.PROTOCOL, "postgresql")
        .option(ConnectionFactoryOptions.USER, postgresCredentials.username)
        .option(ConnectionFactoryOptions.PASSWORD, postgresCredentials.password)
        .option(ConnectionFactoryOptions.DATABASE, flags.database)
        .option(ConnectionFactoryOptions.HOST, flags.host)
        .option(PostgresqlConnectionFactoryProvider.SSL_MODE, SSLMode.REQUIRE)
        .build()
    )
  }
}

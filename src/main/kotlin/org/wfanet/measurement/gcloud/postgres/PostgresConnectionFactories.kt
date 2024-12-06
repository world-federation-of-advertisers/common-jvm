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

import com.google.cloud.sql.core.GcpConnectionFactoryProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import java.time.Duration

object PostgresConnectionFactories {
  @JvmStatic
  fun buildConnectionFactory(flags: PostgresFlags): ConnectionFactory {
    val connectionFactory =
      ConnectionFactories.get(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "gcp")
          .option(ConnectionFactoryOptions.PROTOCOL, "postgresql")
          .option(ConnectionFactoryOptions.USER, flags.user)
          // a non-empty password is required, but the value doesn't matter
          .option(ConnectionFactoryOptions.PASSWORD, "UNUSED")
          .option(ConnectionFactoryOptions.DATABASE, flags.database)
          .option(ConnectionFactoryOptions.HOST, flags.cloudSqlInstance)
          .option(ConnectionFactoryOptions.STATEMENT_TIMEOUT, Duration.ofSeconds(120))
          .option(GcpConnectionFactoryProvider.ENABLE_IAM_AUTH, true)
          .build()
      )

    val configuration: ConnectionPoolConfiguration =
      ConnectionPoolConfiguration.builder(connectionFactory)
        .maxIdleTime(Duration.ofMinutes(5))
        .maxSize(16)
        .build()

    return ConnectionPool(configuration)
  }
}

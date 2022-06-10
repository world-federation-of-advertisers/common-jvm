/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.db.r2dbc.postgres.testing

import com.opentable.db.postgres.embedded.DatabasePreparer
import com.opentable.db.postgres.embedded.PreparedDbProvider
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import java.net.URI
import java.nio.file.Path
import java.util.logging.Level
import javax.sql.DataSource
import kotlinx.coroutines.reactive.awaitFirst
import liquibase.Contexts
import liquibase.Scope
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.db.liquibase.setLogLevel
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.queryMap

class EmbeddedPostgresDatabaseProvider(changelogPath: Path) {
  private val dbProvider: PreparedDbProvider =
    PreparedDbProvider.forPreparer(LiquibasePreparer(changelogPath))

  fun createNewDatabase(): PostgresDatabaseClient {
    val jdbcConnectionUri = URI(dbProvider.createDatabase().removePrefix(JDBC_SCHEME_PREFIX))
    val queryParams: Map<String, String> = jdbcConnectionUri.queryMap
    val connectionFactory =
      ConnectionFactories.get(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "postgresql")
          .option(ConnectionFactoryOptions.HOST, jdbcConnectionUri.host)
          .option(ConnectionFactoryOptions.PORT, jdbcConnectionUri.port)
          .option(ConnectionFactoryOptions.USER, queryParams.getValue("user"))
          .option(ConnectionFactoryOptions.PASSWORD, queryParams.getValue("password"))
          .option(ConnectionFactoryOptions.DATABASE, jdbcConnectionUri.path.trimStart('/'))
          .build()
      )

    return PostgresDatabaseClient { connectionFactory.create().awaitFirst() }
  }

  companion object {
    private const val JDBC_SCHEME_PREFIX = "jdbc:"
  }

  private class LiquibasePreparer(private val changelogPath: Path) : DatabasePreparer {
    override fun prepare(ds: DataSource) {
      ds.connection.use { connection ->
        Liquibase.fromPath(connection, changelogPath).use { liquibase ->
          Scope.getCurrentScope().setLogLevel(Level.FINE)
          liquibase.update(Contexts())
        }
      }
    }
  }
}

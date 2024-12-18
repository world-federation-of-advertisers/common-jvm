/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.db.r2dbc.postgres.testing

import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.reactive.awaitFirst
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.testcontainers.containers.PostgreSQLContainer
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient

/** Provider of PostgreSQL databases. */
interface PostgresDatabaseProvider {
  /** Creates a new database within the instance. */
  fun createDatabase(): PostgresDatabaseClient
}

/**
 * [PostgresDatabaseProvider] implementation as a JUnit [TestRule].
 *
 * This is intended to be used as a [org.junit.ClassRule] so that the underlying container is only
 * started once.
 */
class PostgresDatabaseProviderRule(private val changelogPath: Path) :
  PostgresDatabaseProvider, TestRule {
  private val postgresContainer = KPostgresContainer(POSTGRES_IMAGE_NAME)

  override fun createDatabase() = postgresContainer.createDatabase()

  override fun apply(base: Statement, description: Description): Statement {
    val dbProviderStatement =
      object : Statement() {
        override fun evaluate() {
          postgresContainer.updateTemplateDatabase(changelogPath)
          base.evaluate()
        }
      }
    return (postgresContainer as TestRule).apply(dbProviderStatement, description)
  }

  companion object {
    /** Name of PostgreSQL Docker image. */
    private const val POSTGRES_IMAGE_NAME = "postgres:15"
    private const val TEMPLATE_DATABASE_NAME = "template1"

    private val dbNumber = AtomicInteger()

    private fun KPostgresContainer.updateTemplateDatabase(changelogPath: Path) {
      withDatabaseName(TEMPLATE_DATABASE_NAME).createConnection("").use { connection ->
        Liquibase.update(connection, changelogPath)
      }
    }

    private fun KPostgresContainer.createDatabase(): PostgresDatabaseClient {
      val dbNumber = dbNumber.incrementAndGet()
      val databaseName = "database_$dbNumber"
      createConnection("").use { connection ->
        connection.createStatement().use { it.execute("CREATE DATABASE $databaseName") }
      }

      val connectionFactory =
        ConnectionFactories.get(
          ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .option(ConnectionFactoryOptions.HOST, host)
            .option(
              ConnectionFactoryOptions.PORT,
              getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
            )
            .option(ConnectionFactoryOptions.USER, username)
            .option(ConnectionFactoryOptions.PASSWORD, password)
            .option(ConnectionFactoryOptions.DATABASE, databaseName)
            .build()
        )

      val configuration: ConnectionPoolConfiguration =
        ConnectionPoolConfiguration.builder(connectionFactory)
          .maxIdleTime(Duration.ofMinutes(5))
          .maxSize(16)
          .acquireRetry(10)
          .build()

      return PostgresDatabaseClient { ConnectionPool(configuration).create().awaitFirst() }
    }
  }
}

/** Kotlin generic type for [PostgreSQLContainer]. */
private class KPostgresContainer(imageName: String) :
  PostgreSQLContainer<KPostgresContainer>(imageName)

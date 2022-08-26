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

package org.wfanet.measurement.gcloud.spanner.testing

import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.connection.SpannerPool
import java.nio.file.Path
import java.sql.DriverManager
import java.util.logging.Level
import kotlinx.coroutines.runBlocking
import liquibase.Contexts
import liquibase.Scope
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.db.liquibase.setLogLevel
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.buildSpanner
import org.wfanet.measurement.gcloud.spanner.getAsyncDatabaseClient

/**
 * JUnit rule exposing a temporary Google Cloud Spanner database via Spanner Emulator.
 *
 * @param changelogPath [Path] to a Liquibase changelog.
 */
class SpannerEmulatorDatabaseRule(
  private val changelogPath: Path,
  private val databaseName: String = "test-db"
) : TestRule {
  lateinit var databaseClient: AsyncDatabaseClient
    private set

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        check(!::databaseClient.isInitialized)

        SpannerEmulator().use { emulator ->
          val emulatorHost = runBlocking { emulator.start() }
          createDatabase(emulatorHost).use { spanner ->
            databaseClient =
              spanner.getAsyncDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, databaseName))
            base.evaluate()
          }

          // Make sure these Spanner instances from JDBC are closed before the emulator is shut
          // down, otherwise it will block JVM shutdown.
          SpannerPool.closeSpannerPool()
        }
      }
    }
  }

  private fun createDatabase(emulatorHost: String): Spanner {
    val connectionString =
      SpannerEmulator.buildJdbcConnectionString(emulatorHost, PROJECT, INSTANCE, databaseName)
    DriverManager.getConnection(connectionString).use { connection ->
      Liquibase.fromPath(connection, changelogPath).use { liquibase ->
        Scope.getCurrentScope().setLogLevel(Level.FINE)
        liquibase.update(Contexts())
      }
    }

    return buildSpanner(PROJECT, emulatorHost)
  }

  companion object {
    private const val PROJECT = "test-project"
    private const val INSTANCE = "test-instance"
  }
}

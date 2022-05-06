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

import com.google.cloud.spanner.Database
import com.google.cloud.spanner.DatabaseClient
import java.nio.file.Path
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Level
import liquibase.Contexts
import liquibase.Scope
import org.junit.rules.TestRule
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.db.liquibase.setLogLevel
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.asAsync

/**
 * JUnit rule exposing a temporary Google Cloud Spanner database.
 *
 * All instances share a single [SpannerEmulator].
 *
 * @param changelogPath [Path] to a Liquibase changelog.
 */
class SpannerEmulatorDatabaseRule(changelogPath: Path) :
  DatabaseRule by DatabaseRuleImpl(changelogPath)

private interface DatabaseRule : TestRule {
  val databaseClient: AsyncDatabaseClient
}

private class DatabaseRuleImpl(changelogPath: Path) :
  DatabaseRule, CloseableResource<TemporaryDatabase>({ TemporaryDatabase(changelogPath) }) {

  override val databaseClient: AsyncDatabaseClient
    get() = resource.databaseClient.asAsync()
}

private class TemporaryDatabase(changelogPath: Path) : AutoCloseable {
  private val database: Database
  init {
    val databaseName = "test-db-${instanceCounter.incrementAndGet()}"
    database = emulator.instance.createDatabase(databaseName, listOf()).get()

    val connectionString = emulator.getJdbcConnectionString(database.id)
    DriverManager.getConnection(connectionString).use { connection ->
      Liquibase.fromPath(connection, changelogPath).use { liquibase ->
        Scope.getCurrentScope().setLogLevel(Level.FINE)
        liquibase.update(Contexts())
      }
    }
  }

  val databaseClient: DatabaseClient by lazy { emulator.getDatabaseClient(database.id) }

  override fun close() {
    database.drop()
  }

  companion object {
    /** Atomic counter to ensure each instance has a unique name. */
    private val instanceCounter = AtomicInteger(0)

    private val emulator = EmulatorWithInstance()
  }
}

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
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import java.nio.file.Path
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Level
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
  DatabaseRule by DatabaseRuleImpl(changelogPath) {
  val databaseClient: AsyncDatabaseClient
    get() = runBlocking { getDatabaseClient().asAsync() }
}

private interface DatabaseRule : TestRule {
  suspend fun getDatabaseClient(): DatabaseClient
}

private class DatabaseRuleImpl(changelogPath: Path) :
  DatabaseRule, CloseableResource<TemporaryDatabase>({ TemporaryDatabase(changelogPath) }) {

  override suspend fun getDatabaseClient(): DatabaseClient = resource.getDatabaseClient()
}

private class TemporaryDatabase(private val changelogPath: Path) : AutoCloseable {
  private val databaseName = "test-db-${instanceCounter.incrementAndGet()}"

  private val dbMutex = Mutex()
  @Volatile private lateinit var database: Database

  suspend fun getDatabaseClient(): DatabaseClient {
    val databaseId = getDatabase().id
    return spanner.getDatabaseClient(databaseId)
  }

  private suspend fun getDatabase(): Database {
    if (this::database.isInitialized) {
      return database
    }

    return dbMutex.withLock {
      // Double-checked lock.
      if (this::database.isInitialized) {
        return@withLock database
      }

      val spanner = getSpanner()
      val connectionString = emulator.buildJdbcConnectionString(PROJECT, INSTANCE, databaseName)
      DriverManager.getConnection(connectionString).use { connection ->
        Liquibase.fromPath(connection, changelogPath).use { liquibase ->
          Scope.getCurrentScope().setLogLevel(Level.FINE)
          liquibase.update(Contexts())
        }
      }

      database = spanner.databaseAdminClient.getDatabase(INSTANCE, databaseName)
      database
    }
  }

  override fun close() {
    if (this::database.isInitialized) {
      database.drop()
    }
  }

  companion object {
    private const val PROJECT = "test-project"
    private const val INSTANCE = "test-instance"

    /** Atomic counter to ensure each instance has a unique name. */
    private val instanceCounter = AtomicInteger(0)

    private val emulator = SpannerEmulator()

    @Volatile private lateinit var spanner: Spanner
    private val spannerMutex = Mutex()

    private suspend fun getSpanner(): Spanner {
      if (this::spanner.isInitialized) {
        return spanner
      }

      return spannerMutex.withLock {
        // Double-checked locking.
        if (this::spanner.isInitialized) {
          return@withLock spanner
        }

        val emulatorHost = emulator.start()
        val spannerOptions =
          SpannerOptions.newBuilder().setProjectId(PROJECT).setEmulatorHost(emulatorHost).build()
        spanner = spannerOptions.service
        spanner
      }
    }
  }
}

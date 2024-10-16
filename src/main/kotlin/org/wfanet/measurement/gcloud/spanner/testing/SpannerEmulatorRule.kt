/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.spanner.testing

import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.connection.SpannerPool
import java.nio.file.Path
import java.sql.DriverManager
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.buildSpanner
import org.wfanet.measurement.gcloud.spanner.getAsyncDatabaseClient

/** Administration of databases within a Spanner instance. */
interface SpannerDatabaseAdmin {
  /** Creates a database. */
  fun createDatabase(changelogPath: Path, databaseId: String): AsyncDatabaseClient

  /** Deletes a database. */
  fun deleteDatabase(databaseId: String)
}

/**
 * [TestRule] which manages a [SpannerEmulator] resource.
 *
 * This is intended to be used as a [org.junit.ClassRule].
 */
class SpannerEmulatorRule : TestRule, SpannerDatabaseAdmin {
  private lateinit var emulatorHost: String
  private lateinit var spanner: Spanner

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        SpannerEmulator().use { emulator ->
          try {
            emulatorHost = runBlocking { emulator.start() }
            base.evaluate()
          } finally {
            if (::spanner.isInitialized) {
              spanner.close()
            }
            // Make sure these Spanner instances from JDBC are closed before the emulator is shut
            // down, otherwise it will block JVM shutdown.
            SpannerPool.closeSpannerPool()
          }
        }
      }
    }
  }

  override fun createDatabase(changelogPath: Path, databaseId: String): AsyncDatabaseClient {
    check(::emulatorHost.isInitialized) {
      "Spanner emulator has not been started. " +
        "Ensure that SpannerEmulatorRule has been registered as a ClassRule."
    }

    val connectionString =
      SpannerEmulator.buildJdbcConnectionString(emulatorHost, PROJECT, INSTANCE, databaseId)
    DriverManager.getConnection(connectionString).use { connection ->
      Liquibase.update(connection, changelogPath)
    }

    // Spanner must be initialized only after the first database has been created.
    if (!::spanner.isInitialized) {
      spanner = buildSpanner(PROJECT, emulatorHost)
    }

    return spanner.getAsyncDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, databaseId))
  }

  override fun deleteDatabase(databaseId: String) {
    spanner.databaseAdminClient.dropDatabase(INSTANCE, databaseId)
  }

  companion object {
    private const val PROJECT = "test-project"
    private const val INSTANCE = "test-instance"
  }
}

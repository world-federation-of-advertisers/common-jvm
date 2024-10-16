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

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/**
 * JUnit rule exposing a temporary Google Cloud Spanner database via Spanner Emulator.
 *
 * @param changelogPath [Path] to a Liquibase changelog.
 */
class SpannerEmulatorDatabaseRule(
  private val emulatorDatabaseAdmin: SpannerDatabaseAdmin,
  private val changelogPath: Path,
  private val databaseId: String = "test-db-" + dbCounter.incrementAndGet(),
) : TestRule {
  lateinit var databaseClient: AsyncDatabaseClient
    private set

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        databaseClient = emulatorDatabaseAdmin.createDatabase(changelogPath, databaseId)
        try {
          base.evaluate()
        } finally {
          emulatorDatabaseAdmin.deleteDatabase(databaseId)
        }
      }
    }
  }

  companion object {
    private val dbCounter = AtomicInteger(0)
  }
}

/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.db.liquibase

import com.google.common.truth.Truth.assertThat
import java.nio.file.Path
import kotlin.test.assertFailsWith
import liquibase.exception.CommandValidationException
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.testcontainers.postgresql.PostgreSQLContainer
import org.wfanet.measurement.common.getJarResourcePath

@RunWith(JUnit4::class)
class LiquibaseTest {
  @Test
  fun `update fails when change set has unsupported database`() {
    withPostgres { postgresContainer ->
      postgresContainer.createConnection("").use { connection ->
        val exception =
          assertFailsWith<CommandValidationException> {
            Liquibase.update(connection, INVALID_DBMS_CHANGELOG_PATH)
          }

        assertThat(exception).hasMessageThat().contains("postgresl is not a supported DB")
      }
    }
  }

  @Test
  fun `update applies change set with supported database`() {
    withPostgres { postgresContainer ->
      postgresContainer.createConnection("").use { connection ->
        Liquibase.update(connection, VALID_DBMS_CHANGELOG_PATH)
      }

      postgresContainer.createConnection("").use { connection ->
        connection.createStatement().use { statement ->
          statement.executeQuery("SELECT TestId FROM TestTable").use { resultSet ->
            assertThat(resultSet.next()).isTrue()
            assertThat(resultSet.getLong(1)).isEqualTo(1L)
          }
        }
      }
    }
  }

  private fun withPostgres(block: (PostgreSQLContainer) -> Unit) {
    val postgresContainer = PostgreSQLContainer(POSTGRES_IMAGE_NAME)
    postgresContainer.start()
    try {
      block(postgresContainer)
    } finally {
      postgresContainer.stop()
    }
  }

  companion object {
    private const val POSTGRES_IMAGE_NAME = "postgres:16"
    private val INVALID_DBMS_CHANGELOG_PATH: Path =
      checkNotNull(
        Thread.currentThread()
          .contextClassLoader
          .getJarResourcePath("db/liquibase/invalid-dbms-changelog.sql")
      )
    private val VALID_DBMS_CHANGELOG_PATH: Path =
      INVALID_DBMS_CHANGELOG_PATH.resolveSibling("valid-dbms-changelog.sql")
  }
}

/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.db.r2dbc.postgres

import com.google.common.truth.Truth.assertThat
import com.google.gson.JsonParser
import java.nio.file.Path
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.getJarResourcePath

@RunWith(JUnit4::class)
class PostgresBase64DecoderTest {
  companion object {
    private val CHANGELOG_PATH: Path =
      this::class.java.classLoader.getJarResourcePath("db/postgres/changelog.yaml")!!
    private val dbProvider = EmbeddedPostgresDatabaseProvider(CHANGELOG_PATH)
  }

  private val dbClient = dbProvider.createNewDatabase()

  @Test
  fun `executeStatement returns result with updated rows`() = runBlocking {
    val readWriteContext = dbClient.readWriteTransaction()

    val description = "description"
    val insertStatement =
      boundStatement(
        """
        INSERT INTO CarDescriptions (CarId, Description) VALUES
          (1, $1)
        """
          .trimIndent()
      ) {
        bind("$1", description.toByteArray())
      }

    val insertResult = readWriteContext.executeStatement(insertStatement)
    assertThat(insertResult.numRowsUpdated).isEqualTo(1L)

    val selectStatement =
      boundStatement(
        """
          SELECT
          CarId,
          (
            SELECT JSON_BUILD_OBJECT (
              'Description', encode(Description, 'base64')
            )
          ) as json
          FROM CarDescriptions
        """
          .trimIndent()
      )
    val jsonObj =
      readWriteContext
        .executeQuery(selectStatement)
        .consume { row -> JsonParser.parseString(row["json"]).asJsonObject }
        .first()
    assertThat(jsonObj.getAsJsonPrimitive("Description").decodePostgresBase64().decodeToString())
      .isEqualTo(description)
  }
}

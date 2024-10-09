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

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.spanner.Struct
import com.google.common.truth.Truth.assertThat
import java.nio.file.Path
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule

@RunWith(JUnit4::class)
class AsyncDatabaseClientTest {
  @get:Rule val spannerEmulatorDb = SpannerEmulatorDatabaseRule(CHANGELOG_PATH)

  private lateinit var databaseClient: AsyncDatabaseClient

  @Before
  fun initDatabaseClient() {
    databaseClient = spannerEmulatorDb.databaseClient
  }

  @Test
  fun `executeQuery returns result`() {
    val results: List<Struct> = runBlocking {
      databaseClient.singleUse().executeQuery(statement("SELECT TRUE")).toList()
    }

    assertThat(results.single().getBoolean(0)).isTrue()
  }

  @Test
  fun `run applies buffered mutations`() {
    runBlocking {
      databaseClient.readWriteTransaction().run { txn ->
        txn.bufferInsertMutation("Cars") {
          set("CarId").to(1)
          set("Year").to(1990)
          set("Make").to("Nissan")
          set("Model").to("Stanza")
        }
        txn.bufferInsertMutation("Cars") {
          set("CarId").to(2)
          set("Year").to(1997)
          set("Make").to("Honda")
          set("Model").to("CR-V")
        }
      }
    }

    val results: List<Struct> = runBlocking {
      databaseClient.singleUse().use { txn ->
        txn
          .executeQuery(statement("SELECT CarId, Year, Make, Model FROM Cars ORDER BY CarId"))
          .toList()
      }
    }
    assertThat(results)
      .containsExactly(
        struct {
          set("CarId").to(1)
          set("Year").to(1990)
          set("Make").to("Nissan")
          set("Model").to("Stanza")
        },
        struct {
          set("CarId").to(2)
          set("Year").to(1997)
          set("Make").to("Honda")
          set("Model").to("CR-V")
        },
      )
      .inOrder()
  }

  @Test
  fun `run executes statement`() {
    val statementSql =
      """
      INSERT INTO Cars(CarId, Year, Make, Model)
      VALUES
        (1, 1990, 'Nissan', 'Stanza'),
        (2, 1997, 'Honda', 'CR-V')
      """
        .trimIndent()

    runBlocking {
      databaseClient.readWriteTransaction().run { txn ->
        txn.executeUpdate(statement(statementSql))
      }
    }

    val results: List<Struct> = runBlocking {
      databaseClient.singleUse().use { txn ->
        txn
          .executeQuery(statement("SELECT CarId, Year, Make, Model FROM Cars ORDER BY CarId"))
          .toList()
      }
    }
    assertThat(results)
      .containsExactly(
        struct {
          set("CarId").to(1)
          set("Year").to(1990)
          set("Make").to("Nissan")
          set("Model").to("Stanza")
        },
        struct {
          set("CarId").to(2)
          set("Year").to(1997)
          set("Make").to("Honda")
          set("Model").to("CR-V")
        },
      )
      .inOrder()
  }

  @Test
  fun `run bubbles exceptions from transaction work`() = runBlocking {
    val message = "Error inside transaction work"

    val exception =
      assertFailsWith<Exception> {
        databaseClient.readWriteTransaction().run { _ -> throw Exception(message) }
      }

    assertThat(exception).hasMessageThat().isEqualTo(message)
  }

  companion object {
    private const val CHANGELOG_RESOURCE_NAME = "db/spanner/changelog.yaml"
    private val CHANGELOG_PATH: Path =
      requireNotNull(this::class.java.classLoader.getJarResourcePath(CHANGELOG_RESOURCE_NAME)) {
        "Resource $CHANGELOG_RESOURCE_NAME not found"
      }
  }
}

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

package org.wfanet.measurement.common.db.r2dbc.postgres

import com.google.common.truth.Truth.assertThat
import io.r2dbc.postgresql.api.ErrorDetails
import io.r2dbc.postgresql.api.PostgresqlException
import io.r2dbc.spi.R2dbcRollbackException
import java.nio.file.Path
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.RandomIdGenerator

@RunWith(JUnit4::class)
class PostgresWriterTest {
  private val dbClient = databaseProvider.createDatabase()

  private suspend fun readCarIds(): List<InternalId> =
    dbClient
      .singleUse()
      .executeQuery(boundStatement("SELECT CarId FROM Cars ORDER BY CarId"))
      .consume<InternalId> { row -> row["CarId"] }
      .toList()

  private suspend fun readCarYear(carId: InternalId): Int =
    dbClient
      .singleUse()
      .executeQuery(boundStatement("SELECT Year FROM Cars WHERE CarId = $1") { bind("$1", carId) })
      .consume<Int> { row -> row["Year"] }
      .toList()
      .first()

  @Test
  fun `execute retries serialization failure`() = runBlocking {
    val writer = InsertCarsWriter(failFirstAttempt = true)

    writer.execute(dbClient, ID_GENERATOR)

    assertThat(writer.attempts).isEqualTo(2)
    assertThat(readCarIds()).containsExactly(FIRST_CAR_ID, SECOND_CAR_ID).inOrder()
  }

  @Test
  fun `execute does not commit statements of a retried attempt individually`(): Unit = runBlocking {
    val firstStatementExecuted = CompletableDeferred<Unit>()
    val resumeAttempt = CompletableDeferred<Unit>()
    val writer =
      InsertCarsWriter(
        failFirstAttempt = true,
        betweenStatements = {
          firstStatementExecuted.complete(Unit)
          resumeAttempt.await()
        },
      )

    coroutineScope {
      val execution = async { writer.execute(dbClient, ID_GENERATOR) }
      firstStatementExecuted.await()

      // The retried attempt has executed its first statement but has not committed.
      assertThat(readCarIds()).isEmpty()

      resumeAttempt.complete(Unit)
      execution.await()
    }

    assertThat(readCarIds()).containsExactly(FIRST_CAR_ID, SECOND_CAR_ID).inOrder()
  }

  @Test
  fun `execute retries when another transaction updates the same row`(): Unit = runBlocking {
    InsertCarsWriter().execute(dbClient, ID_GENERATOR)

    val rowRead = CompletableDeferred<Unit>()
    val resumeAttempt = CompletableDeferred<Unit>()
    val writer =
      UpdateCarYearWriter(
        year = 2022,
        afterRead = {
          rowRead.complete(Unit)
          resumeAttempt.await()
        },
      )

    coroutineScope {
      val execution = async { writer.execute(dbClient, ID_GENERATOR) }
      rowRead.await()

      // Updating the row that the other transaction has read causes it to fail to serialize.
      UpdateCarYearWriter(year = 2021).execute(dbClient, ID_GENERATOR)

      resumeAttempt.complete(Unit)
      execution.await()
    }

    assertThat(writer.attempts).isEqualTo(2)
    assertThat(readCarYear(FIRST_CAR_ID)).isEqualTo(2022)
  }

  /** [PostgresWriter] which inserts two Cars using a statement for each. */
  private class InsertCarsWriter(
    private val failFirstAttempt: Boolean = false,
    private val betweenStatements: suspend () -> Unit = {},
  ) : PostgresWriter<Unit>() {
    var attempts: Int = 0
      private set

    override suspend fun TransactionScope.runTransaction() {
      attempts++
      transactionContext.executeStatement(insertCarStatement(FIRST_CAR_ID))
      if (failFirstAttempt && attempts == 1) {
        throw SimulatedSerializationFailure()
      }
      betweenStatements()
      transactionContext.executeStatement(insertCarStatement(SECOND_CAR_ID))
    }

    companion object {
      private fun insertCarStatement(carId: InternalId) =
        boundStatement(
          "INSERT INTO Cars (CarId, Year, Make, Model) VALUES ($1, $2, 'Tesla', 'Model 3')"
        ) {
          bind("$1", carId)
          bind("$2", INITIAL_YEAR)
        }
    }
  }

  /** [PostgresWriter] which reads a Car and then updates its Year. */
  private class UpdateCarYearWriter(
    private val year: Int,
    private val afterRead: suspend () -> Unit = {},
  ) : PostgresWriter<Unit>() {
    var attempts: Int = 0
      private set

    override suspend fun TransactionScope.runTransaction() {
      attempts++
      transactionContext
        .executeQuery(
          boundStatement("SELECT Year FROM Cars WHERE CarId = $1") { bind("$1", FIRST_CAR_ID) }
        )
        .consume<Int> { row -> row["Year"] }
        .toList()
      afterRead()
      transactionContext.executeStatement(
        boundStatement("UPDATE Cars SET Year = $1 WHERE CarId = $2") {
          bind("$1", year)
          bind("$2", FIRST_CAR_ID)
        }
      )
    }
  }

  /**
   * [PostgresqlException] for a serialization failure, as thrown by the driver when a transaction
   * cannot be serialized.
   */
  private class SimulatedSerializationFailure :
    R2dbcRollbackException(MESSAGE, SERIALIZATION_FAILURE_SQL_STATE, 0), PostgresqlException {
    override fun getErrorDetails(): ErrorDetails =
      ErrorDetails.fromCodeAndMessage(SERIALIZATION_FAILURE_SQL_STATE, MESSAGE)

    companion object {
      private const val MESSAGE = "simulated serialization failure"
    }
  }

  companion object {
    private const val SERIALIZATION_FAILURE_SQL_STATE = "40001"
    private const val INITIAL_YEAR = 2020

    private val FIRST_CAR_ID = InternalId(1L)
    private val SECOND_CAR_ID = InternalId(2L)
    private val ID_GENERATOR = RandomIdGenerator()

    private val CHANGELOG_PATH: Path =
      this::class.java.classLoader.getJarResourcePath("db/postgres/changelog.yaml")!!

    @get:ClassRule @JvmStatic val databaseProvider = PostgresDatabaseProviderRule(CHANGELOG_PATH)
  }
}

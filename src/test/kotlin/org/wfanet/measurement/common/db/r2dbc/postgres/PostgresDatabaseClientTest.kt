/*
 * Copyright 2022 The Cross-Media Measurement Authors
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
import com.google.type.DayOfWeek
import com.google.type.LatLng
import com.google.type.latLng
import java.nio.file.Path
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.ReadWriteContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.identity.InternalId

@RunWith(JUnit4::class)
class PostgresDatabaseClientTest {
  private val dbClient = databaseProvider.createDatabase()

  @Test
  fun `executeStatement returns result with updated rows`() {
    val insertStatement =
      boundStatement(
        """
        INSERT INTO Cars (CarId, Year, Make, Model) VALUES
          (1, 1990, 'Nissan', 'Stanza'),
          (2, 1997, 'Honda', 'CR-V'),
          (3, 2012, 'Audi', 'S4'),
          (4, 2020, 'Tesla', 'Model 3')
        """
          .trimIndent()
      )

    val result = runBlocking {
      val txn = dbClient.readWriteTransaction()
      try {
        txn.executeStatement(insertStatement)
      } finally {
        txn.close()
      }
    }

    assertThat(result.numRowsUpdated).isEqualTo(4L)
  }

  @Test
  fun `bind binds parameters by name`(): Unit = runBlocking {
    val car =
      Car(
        InternalId(123L),
        2020,
        "Tesla",
        "Model 3",
        null,
        latLng {
          latitude = 33.995325
          longitude = -118.477021
        },
        DayOfWeek.TUESDAY,
      )
    val insertStatement =
      boundStatement("INSERT INTO Cars VALUES ($1, $2, $3, $4, $5, $6, $7)") {
        bind("$1", car.carId)
        bind("$2", car.year)
        bind("$3", car.make)
        bind("$4", car.model)
        bind("$5", car.owner)
        bind("$6", car.currentLocation)
        bind("$7", car.weeklyWashDay)
      }
    with(dbClient.readWriteTransaction()) {
      executeStatement(insertStatement)
      commit()
    }

    val query = boundStatement("SELECT * FROM Cars")
    val cars: Flow<Car> =
      dbClient.singleUse().executeQuery(query).consume { row -> Car.parseFrom(row) }

    assertThat(cars.toList()).containsExactly(car)
  }

  @Test
  fun `addBinding adds bindings`() = runBlocking {
    val cars =
      listOf(
        Car(carId = InternalId(1), year = 2012, make = "Audi", model = "S4"),
        Car(carId = InternalId(2), year = 2020, make = "Tesla", model = "Model 3"),
      )
    val insertStatement =
      boundStatement("INSERT INTO Cars (CarId, Year, Make, Model) VALUES ($1, $2, $3, $4)") {
        for (car in cars) {
          addBinding {
            bind("$1", car.carId)
            bind("$2", car.year)
            bind("$3", car.make)
            bind("$4", car.model)
          }
        }
      }

    val statementResult =
      with(dbClient.readWriteTransaction()) { executeStatement(insertStatement).also { commit() } }
    assertThat(statementResult.numRowsUpdated).isEqualTo(2L)

    val query = boundStatement("SELECT * FROM Cars ORDER BY CarId")
    val result: Flow<Car> =
      dbClient.singleUse().executeQuery(query).consume { row -> Car.parseFrom(row) }

    assertThat(result.toList()).containsExactlyElementsIn(cars).inOrder()
  }

  @Test
  fun `valuesListBoundStatement can be used to execute insert with values list`() = runBlocking {
    val cars =
      listOf(
        Car(carId = InternalId(1), year = 2012, make = "Audi", model = "S4"),
        Car(carId = InternalId(2), year = 2020, make = "Tesla", model = "Model 3"),
      )
    val insertStatement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 4,
        "INSERT INTO Cars (CarId, Year, Make, Model) VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}",
      ) {
        for (car in cars) {
          addValuesBinding {
            bindValuesParam(0, car.carId)
            bindValuesParam(1, car.year)
            bindValuesParam(2, car.make)
            bindValuesParam(3, car.model)
          }
        }
      }

    val statementResult =
      with(dbClient.readWriteTransaction()) { executeStatement(insertStatement).also { commit() } }
    assertThat(statementResult.numRowsUpdated).isEqualTo(2L)

    val query = boundStatement("SELECT * FROM Cars ORDER BY CarId")
    val result: Flow<Car> =
      dbClient.singleUse().executeQuery(query).consume { row -> Car.parseFrom(row) }

    assertThat(result.toList()).containsExactlyElementsIn(cars).inOrder()
  }

  @Test
  fun `valuesListBoundStatement can be used to execute update with values list`() = runBlocking {
    val cars =
      listOf(
        Car(carId = InternalId(1), year = 2020, make = "Audi", model = "S4"),
        Car(carId = InternalId(2), year = 2020, make = "Tesla", model = "Model 3"),
      )
    val insertStatement =
      boundStatement("INSERT INTO Cars (CarId, Year, Make, Model) VALUES ($1, $2, $3, $4)") {
        for (car in cars) {
          addBinding {
            bind("$1", car.carId)
            bind("$2", car.year)
            bind("$3", car.make)
            bind("$4", car.model)
          }
        }
      }

    val insertStatementResult =
      with(dbClient.readWriteTransaction()) { executeStatement(insertStatement).also { commit() } }
    assertThat(insertStatementResult.numRowsUpdated).isEqualTo(2L)

    val updatedCars =
      listOf(
        Car(carId = InternalId(1), year = 2020, make = "A", model = "A4"),
        Car(carId = InternalId(2), year = 2020, make = "T", model = "Model Y"),
      )

    val updateStatement =
      valuesListBoundStatement(
        valuesStartIndex = 1,
        paramCount = 3,
        """
        UPDATE Cars as c SET Make = u.Make, Model = u.Model
        FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
        AS u(CarId, Make, Model)
        WHERE Year = $1 and c.CarId = u.CarId
      """
          .trimIndent(),
      ) {
        bind("$1", 2020)
        for (car in updatedCars) {
          addValuesBinding {
            bindValuesParam(0, car.carId)
            bindValuesParam(1, car.make)
            bindValuesParam(2, car.model)
          }
        }
      }

    val updateStatementResult =
      with(dbClient.readWriteTransaction()) { executeStatement(updateStatement).also { commit() } }
    assertThat(updateStatementResult.numRowsUpdated).isEqualTo(2L)

    val query = boundStatement("SELECT * FROM Cars ORDER BY CarId")
    val result: Flow<Car> =
      dbClient.singleUse().executeQuery(query).consume { row -> Car.parseFrom(row) }

    assertThat(result.toList()).containsExactlyElementsIn(updatedCars).inOrder()
  }

  @Test
  fun `rollbackTransaction rolls back the transaction`() = runBlocking {
    val car = Car(carId = InternalId(2), year = 2020, make = "Tesla", model = "Model 3")
    val insertStatement =
      boundStatement("INSERT INTO Cars (CarId, Year, Make, Model) VALUES ($1, $2, $3, $4)") {
        addBinding {
          bind("$1", car.carId)
          bind("$2", car.year)
          bind("$3", car.make)
          bind("$4", car.model)
        }
      }

    val readWriteContext = dbClient.readWriteTransaction()
    val insertResult = readWriteContext.executeStatement(insertStatement)
    assertThat(insertResult.numRowsUpdated).isEqualTo(1L)

    val query = boundStatement("SELECT * FROM Cars")
    var selectResult: Flow<Car> =
      readWriteContext.executeQuery(query).consume { row -> Car.parseFrom(row) }
    assertThat(selectResult.toList()).hasSize(1)

    readWriteContext.rollback()
    selectResult = readWriteContext.executeQuery(query).consume { row -> Car.parseFrom(row) }
    assertThat(selectResult.toList()).hasSize(0)
  }

  @Test
  fun `executeQuery reads writes from same transaction`(): Unit = runBlocking {
    val insertStatement =
      boundStatement(
        """
        INSERT INTO Cars (CarId, Year, Make, Model) VALUES
          (5, 2021, 'Tesla', 'Model Y'),
          (1, 1990, 'Nissan', 'Stanza'),
          (2, 1997, 'Honda', 'CR-V'),
          (3, 2012, 'Audi', 'S4'),
          (4, 2020, 'Tesla', 'Model 3')
        """
          .trimIndent()
      )
    val txn: ReadWriteContext = dbClient.readWriteTransaction()
    txn.executeStatement(insertStatement)

    val query = boundStatement("SELECT * FROM Cars ORDER BY Year ASC")
    val models: Flow<String> =
      txn.executeQuery(query).consume<String> { row -> row["Model"] }.onCompletion { txn.close() }

    assertThat(models.toList())
      .containsExactly("Stanza", "CR-V", "S4", "Model 3", "Model Y")
      .inOrder()
  }

  @Test
  fun `executeQuery does not see writes from pending write transaction`(): Unit = runBlocking {
    val insertStatement =
      boundStatement(
        """
        INSERT INTO Cars (CarId, Year, Make, Model) VALUES
          (5, 2021, 'Tesla', 'Model Y'),
          (1, 1990, 'Nissan', 'Stanza'),
          (2, 1997, 'Honda', 'CR-V'),
          (3, 2012, 'Audi', 'S4'),
          (4, 2020, 'Tesla', 'Model 3')
        """
          .trimIndent()
      )
    val writeTxn: ReadWriteContext = dbClient.readWriteTransaction()
    writeTxn.executeStatement(insertStatement)

    val query = boundStatement("SELECT * FROM CARS")
    val models: Flow<String> =
      with(dbClient.readTransaction()) {
        executeQuery(query).consume<String> { row -> row["Model"] }.onCompletion { close() }
      }
    writeTxn.close()

    assertThat(models.toList()).isEmpty()
  }

  companion object {
    private val CHANGELOG_PATH: Path =
      this::class.java.classLoader.getJarResourcePath("db/postgres/changelog.yaml")!!

    @get:ClassRule @JvmStatic val databaseProvider = PostgresDatabaseProviderRule(CHANGELOG_PATH)
  }
}

private data class Car(
  val carId: InternalId,
  val year: Long,
  val make: String,
  val model: String,
  val owner: String? = null,
  val currentLocation: LatLng? = null,
  val weeklyWashDay: DayOfWeek = DayOfWeek.DAY_OF_WEEK_UNSPECIFIED,
) {
  companion object {
    fun parseFrom(row: ResultRow): Car {
      return with(row) {
        Car(
          get("CarId"),
          get("Year"),
          get("Make"),
          get("Model"),
          get("Owner"),
          getProtoMessage("CurrentLocation", LatLng.parser()),
          getProtoEnum("WeeklyWashDay", DayOfWeek::forNumber),
        )
      }
    }
  }
}

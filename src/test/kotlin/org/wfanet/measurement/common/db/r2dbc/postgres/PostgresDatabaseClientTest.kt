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
import com.google.type.LatLng
import com.google.type.latLng
import io.r2dbc.spi.Readable
import java.nio.file.Path
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.ReadWriteContext
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder.Companion.statementBuilder
import org.wfanet.measurement.common.db.r2dbc.get
import org.wfanet.measurement.common.db.r2dbc.getProtoMessageOrNull
import org.wfanet.measurement.common.db.r2dbc.getValue
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.EmbeddedPostgresDatabaseProvider
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.common.identity.InternalId

@RunWith(JUnit4::class)
class PostgresDatabaseClientTest {
  private val dbClient = dbProvider.createNewDatabase()

  @Test
  fun `executeStatement returns result with updated rows`() {
    val insertStatement =
      StatementBuilder(
        """
        INSERT INTO Cars (CarId, Year, Make, Model) VALUES
          (1, 1990, 'Nissan', 'Stanza'),
          (2, 1997, 'Honda', 'CR-V'),
          (3, 2012, 'Audi', 'S4'),
          (4, 2020, 'Tesla', 'Model 3')
        """.trimIndent()
      )

    val result = runBlocking {
      val txn = dbClient.readWriteTransaction()
      try {
        txn.executeStatement(insertStatement)
      } finally {
        txn.close()
      }
    }

    assertThat(result.numRowsUpdated).isEqualTo(4)
  }

  @Test
  fun `bind binds parameters by name`(): Unit = runBlocking {
    val car =
      Car(
        InternalId(123L),
        2020,
        "Tesla",
        "Model 3",
        "Bob",
        latLng {
          latitude = 33.995325
          longitude = -118.477021
        }
      )
    val insertStatement =
      statementBuilder("INSERT INTO Cars VALUES ($1, $2, $3, $4, $5, $6)") {
        bind("$1", car.carId)
        bind("$2", car.year)
        bind("$3", car.make)
        bind("$4", car.model)
        bind("$5", car.owner!!)
        bind("$6", car.currentLocation!!)
      }
    with(dbClient.readWriteTransaction()) {
      executeStatement(insertStatement)
      commit()
    }

    val query = statementBuilder("SELECT * FROM Cars")
    val cars: Flow<Car> =
      dbClient.singleUse().executeQuery(query).consume { row -> Car.parseFrom(row) }

    assertThat(cars.toList()).containsExactly(car)
  }

  @Test
  fun `bindNull binds parameter to null`(): Unit = runBlocking {
    val insertStatement =
      statementBuilder(
        """
        INSERT INTO Cars (CarId, Year, Make, Model, Owner) VALUES
          (1, 1990, 'Nissan', 'Stanza', $1),
          (2, 1997, 'Honda', 'CR-V', $2),
          (3, 2012, 'Audi', 'S4', $3),
          (4, 2020, 'Tesla', 'Model 3', $4)
        """.trimIndent()
      ) {
        bind("$1", "Alice")
        bindNull<String>("$2")
        bind("$3", "Carol")
        bindNull<String>("$4")
      }
    with(dbClient.readWriteTransaction()) {
      executeStatement(insertStatement)
      commit()
    }

    val query = statementBuilder("SELECT * FROM Cars WHERE Owner IS NULL ORDER BY Year DESC")
    val models: Flow<String> =
      dbClient.singleUse().executeQuery(query).consume { row -> row.getValue("Model") }

    assertThat(models.toList()).containsExactly("Model 3", "CR-V").inOrder()
  }

  @Test
  fun `executeQuery reads writes from same transaction`(): Unit = runBlocking {
    val insertStatement =
      statementBuilder(
        """
        INSERT INTO Cars (CarId, Year, Make, Model) VALUES
          (5, 2021, 'Tesla', 'Model Y'),
          (1, 1990, 'Nissan', 'Stanza'),
          (2, 1997, 'Honda', 'CR-V'),
          (3, 2012, 'Audi', 'S4'),
          (4, 2020, 'Tesla', 'Model 3')
        """.trimIndent()
      )
    val txn: ReadWriteContext = dbClient.readWriteTransaction()
    txn.executeStatement(insertStatement)

    val query = statementBuilder("SELECT * FROM Cars ORDER BY Year ASC")
    val models: Flow<String> =
      txn.executeQuery(query).consume { row -> row.getValue<String>("Model") }.onCompletion {
        txn.close()
      }

    assertThat(models.toList())
      .containsExactly("Stanza", "CR-V", "S4", "Model 3", "Model Y")
      .inOrder()
  }

  @Test
  fun `executeQuery does not see writes from pending write transaction`(): Unit = runBlocking {
    val insertStatement =
      statementBuilder(
        """
        INSERT INTO Cars (CarId, Year, Make, Model) VALUES
          (5, 2021, 'Tesla', 'Model Y'),
          (1, 1990, 'Nissan', 'Stanza'),
          (2, 1997, 'Honda', 'CR-V'),
          (3, 2012, 'Audi', 'S4'),
          (4, 2020, 'Tesla', 'Model 3')
        """.trimIndent()
      )
    val writeTxn: ReadWriteContext = dbClient.readWriteTransaction()
    writeTxn.executeStatement(insertStatement)

    val query = statementBuilder("SELECT * FROM CARS")
    val models: Flow<String> =
      with(dbClient.readTransaction()) {
        executeQuery(query).consume { row -> row.getValue<String>("Model") }.onCompletion {
          close()
        }
      }
    writeTxn.close()

    assertThat(models.toList()).isEmpty()
  }

  companion object {
    private val CHANGELOG_PATH: Path =
      this::class.java.classLoader.getJarResourcePath("db/postgres/changelog.yaml")!!
    private val dbProvider = EmbeddedPostgresDatabaseProvider(CHANGELOG_PATH)
  }
}

private data class Car(
  val carId: InternalId,
  val year: Long,
  val make: String,
  val model: String,
  val owner: String? = null,
  val currentLocation: LatLng? = null
) {
  companion object {
    fun parseFrom(row: Readable): Car {
      return with(row) {
        Car(
          getValue("CarId"),
          getValue("Year"),
          getValue("Make"),
          getValue("Model"),
          get<String>("Owner"),
          getProtoMessageOrNull("CurrentLocation", LatLng.parser())
        )
      }
    }
  }
}

// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.db.r2dbc.postgres

import io.r2dbc.postgresql.api.PostgresqlException
import kotlin.random.Random
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.flow.single

object SerializableErrors {
  private const val SERIALIZABLE_ERROR_CODE = "40001"

  suspend fun <T> retrying(block: suspend () -> T): T {
    return flow { emit(block()) }.withSerializableErrorRetries().single()
  }

  fun <T> Flow<T>.withSerializableErrorRetries(): Flow<T> {
    return this.retry { e ->
      (e is PostgresqlException && e.errorDetails.code == SERIALIZABLE_ERROR_CODE).also {
        delay(Random.nextLong(50, 100))
      }
    }
  }
}

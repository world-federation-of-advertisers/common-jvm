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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule

@RunWith(JUnit4::class)
class AsyncDatabaseClientTest {
  @get:Rule val database = SpannerEmulatorDatabaseRule(spannerEmulator, CHANGELOG_PATH)

  private val databaseClient: AsyncDatabaseClient
    get() = database.databaseClient

  @Test
  fun `executes simple query`() {
    val results: List<Struct> = runBlocking {
      databaseClient.singleUse().executeQuery(statement("SELECT TRUE") {}).toList()
    }

    assertThat(results.single().getBoolean(0)).isTrue()
  }

  companion object {
    private const val CHANGELOG_RESOURCE_NAME = "db/spanner/changelog.yaml"
    private val CHANGELOG_PATH: Path =
      requireNotNull(this::class.java.classLoader.getJarResourcePath(CHANGELOG_RESOURCE_NAME)) {
        "Resource $CHANGELOG_RESOURCE_NAME not found"
      }

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}

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
import org.junit.Rule
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/**
 * Base class for JUnit4 tests using Cloud Spanner databases running in a test [Instance]
 * [com.google.cloud.spanner.Instance] in Cloud Spanner Emulator.
 *
 * One emulator and test instance is created per test class, and one database is created per test
 * case method. The [AsyncDatabaseClient] is accessible via the [databaseClient] property.
 *
 * Example use:
 * ```
 * class MySpannerTest : UsingSpannerEmulator("/path/to/spanner/schema/resource") {
 *   @Test
 *   fun mutationsGonnaMutate() {
 *     databaseClient.write(buildMutations())
 *
 *     val result = databaseClient.singleUse().executeQuery(buildQuery)
 *     // Make assertions about the results
 *     // ...
 *   }
 * }
 * ```
 */
abstract class UsingSpannerEmulator(changeLogResourcePath: Path) {
  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(changeLogResourcePath)

  val databaseClient: AsyncDatabaseClient
    get() = spannerDatabase.databaseClient
}

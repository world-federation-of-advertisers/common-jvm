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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import java.time.Period
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/* Test for [LocalDataProgression]. */
@RunWith(JUnit4::class)
class LocalDateProgressionTest {
  @Test
  fun `iteration includes all dates in closed range`() {
    val today = LocalDate.now()
    val lastWeek = today.minusWeeks(1)

    val dates = (lastWeek..today).toList()

    assertThat(dates)
      .containsExactly(
        lastWeek,
        lastWeek.plusDays(1),
        lastWeek.plusDays(2),
        lastWeek.plusDays(3),
        lastWeek.plusDays(4),
        lastWeek.plusDays(5),
        lastWeek.plusDays(6),
        today,
      )
      .inOrder()
  }

  @Test
  fun `iteration with step includes all expected dates`() {
    val today = LocalDate.now()
    val eightDaysAgo = today.minusDays(8)

    val dates = (eightDaysAgo..today step Period.ofDays(2)).toList()

    assertThat(dates)
      .containsExactly(
        eightDaysAgo,
        eightDaysAgo.plusDays(2),
        eightDaysAgo.plusDays(4),
        eightDaysAgo.plusDays(6),
        today,
      )
      .inOrder()
  }

  @Test
  fun `iteration with step not divisible by size excludes range end`() {
    val today = LocalDate.now()
    val lastWeek = today.minusWeeks(1)

    val dates = (lastWeek..today step Period.ofDays(2)).toList()

    assertThat(dates)
      .containsExactly(lastWeek, lastWeek.plusDays(2), lastWeek.plusDays(4), lastWeek.plusDays(6))
      .inOrder()
  }
}

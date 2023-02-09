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
import java.time.Instant
import java.time.LocalDate
import java.time.Period
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/* Test for [OpenEndTimeRange]. */
@RunWith(JUnit4::class)
class OpenEndTimeRangeTest {
  @Test
  fun `contains returns true for start`() {
    val now = Instant.now()
    val lastWeek = now.minus(Period.ofWeeks(1))

    assertThat(lastWeek in OpenEndTimeRange(lastWeek, now)).isTrue()
  }

  @Test
  fun `contains returns false for endExclusive`() {
    val now = Instant.now()
    val lastWeek = now.minus(Period.ofWeeks(1))

    assertThat(now in OpenEndTimeRange(lastWeek, now)).isFalse()
  }

  @Test
  fun `contains returns true for time between start and end`() {
    val now = Instant.now()
    val lastWeek = now.minus(Period.ofWeeks(1))

    assertThat(now.minusSeconds(1) in OpenEndTimeRange(lastWeek, now)).isTrue()
  }

  @Test
  fun `overlaps returns true for fully contained range`() {
    val now = Instant.now()
    val lastWeek = now.minus(Period.ofWeeks(1))
    val subject = OpenEndTimeRange(lastWeek, now)
    val other = OpenEndTimeRange(lastWeek.plus(Period.ofDays(1)), now.minus(Period.ofDays(1)))

    assertThat(subject.overlaps(other)).isTrue()
  }

  @Test
  fun `overlaps returns true for partially overlapping range`() {
    val now = Instant.now()
    val lastWeek = now.minus(Period.ofWeeks(1))
    val subject = OpenEndTimeRange(lastWeek, now)
    val other = OpenEndTimeRange(lastWeek.minus(Period.ofDays(1)), lastWeek.plus(Period.ofDays(1)))

    assertThat(subject.overlaps(other)).isTrue()
  }

  @Test
  fun `overlaps returns false for range that only overlaps end`() {
    val now = Instant.now()
    val lastWeek = now.minus(Period.ofWeeks(1))
    val subject = OpenEndTimeRange(lastWeek, now)
    val other = OpenEndTimeRange(now, now.plus(Period.ofDays(1)))

    assertThat(subject.overlaps(other)).isFalse()
  }

  @Test
  fun `fromClosedDateRange returns range from start of first day to start of day after last day`() {
    val firstDay = LocalDate.of(2023, 2, 1)
    val lastDay = LocalDate.of(2023, 2, 8)

    val range = OpenEndTimeRange.fromClosedDateRange(firstDay..lastDay)

    assertThat(range.start).isEqualTo(Instant.parse("2023-02-01T00:00:00Z"))
    assertThat(range.endExclusive).isEqualTo(Instant.parse("2023-02-09T00:00:00Z"))
  }
}

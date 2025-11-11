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

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

/** Represents a range of [Instant]s where the upper bound is not included in the range. */
@ConsistentCopyVisibility
@Deprecated("Prefer OpenEndRange<Instant>")
data class OpenEndTimeRange private constructor(private val delegate: OpenEndRange<Instant>) :
  OpenEndRange<Instant> by delegate {

  @Deprecated("Prefer OpenEndRange<Instant>", ReplaceWith("start..<endExclusive"))
  constructor(start: Instant, endExclusive: Instant) : this(start..<endExclusive)

  fun overlaps(@Suppress("DEPRECATION") other: OpenEndTimeRange): Boolean =
    overlaps(other as OpenEndRange<Instant>)

  override fun toString(): String = delegate.toString()

  companion object {
    @Suppress("DEPRECATION")
    @Deprecated(
      "Use dateRange.toOpenEndInstantRange",
      ReplaceWith(
        "dateRange.toOpenEndInstantRange(zoneOffset)",
        imports = ["org.wfanet.measurement.common.toOpenEndInstantRange"],
      ),
    )
    fun fromClosedDateRange(
      dateRange: ClosedRange<LocalDate>,
      zoneOffset: ZoneOffset = ZoneOffset.UTC,
    ) = OpenEndTimeRange(dateRange.toOpenEndInstantRange(zoneOffset))
  }
}

fun OpenEndRange<Instant>.overlaps(other: OpenEndRange<Instant>): Boolean {
  return start.coerceAtLeast(other.start) < endExclusive.coerceAtMost(other.endExclusive)
}

operator fun OpenEndRange<Instant>.contains(other: OpenEndRange<Instant>): Boolean {
  return start <= other.start && endExclusive >= other.endExclusive
}

fun ClosedRange<LocalDate>.toOpenEndInstantRange(
  zoneOffset: ZoneOffset = ZoneOffset.UTC
): OpenEndRange<Instant> {
  return start.atStartOfDay().toInstant(zoneOffset)..<endInclusive
      .plusDays(1)
      .atStartOfDay()
      .toInstant(zoneOffset)
}

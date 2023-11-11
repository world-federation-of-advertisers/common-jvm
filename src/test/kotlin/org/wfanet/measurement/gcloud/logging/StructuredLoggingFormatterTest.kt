/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.logging

import com.google.common.truth.Truth.assertThat
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.logging.Logger
import java.util.logging.StreamHandler
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class StructuredLoggingFormatterTest {
  private val logger = Logger.getLogger(this::class.java.name)
  private val logStream = ByteArrayOutputStream()
  private val loggingHandler = StreamHandler(logStream, StructuredLoggingFormatter())

  @Before
  fun setLoggingHandler() {
    logger.useParentHandlers = false
    logger.addHandler(loggingHandler)
  }

  @Test
  fun `logs entry as structured JSON`() {
    val innerClass: Class<*> = object {}.javaClass
    val qualifiedFunctionName =
      "${innerClass.enclosingClass.name}.${innerClass.enclosingMethod.name}"
    val start = Instant.ofEpochMilli(System.currentTimeMillis())
    val message = "Log message"

    logger.info(message)

    loggingHandler.flush()
    val log = logStream.toString()
    assertThat(log).endsWith(System.lineSeparator())
    val entryPayload: JsonObject = JsonParser.parseString(log).asJsonObject
    assertThat(entryPayload[StructuredLoggingFormatter.LogFields.MESSAGE].asString)
      .isEqualTo(message)
    assertThat(Instant.parse(entryPayload[StructuredLoggingFormatter.LogFields.TIME].asString))
      .isAtLeast(start)
    assertThat(entryPayload[StructuredLoggingFormatter.LogFields.SEVERITY].asString)
      .isEqualTo(LogSeverity.INFO.name)
    assertThat(
        entryPayload[StructuredLoggingFormatter.LogFields.SOURCE_LOCATION].asJsonObject.asMap()
      )
      .containsExactly("function", JsonPrimitive(qualifiedFunctionName))
  }
}

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

import com.google.gson.JsonObject
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Instant
import java.util.logging.Formatter
import java.util.logging.Level
import java.util.logging.LogRecord
import org.jetbrains.annotations.VisibleForTesting

/**
 * Logging [Formatter] for structured logging.
 *
 * See https://cloud.google.com/logging/docs/structured-logging#special-payload-fields
 */
class StructuredLoggingFormatter : Formatter() {
  @VisibleForTesting
  object LogFields {
    const val SEVERITY = "severity"
    const val MESSAGE = "message"
    const val TIME = "time"
    const val SOURCE_LOCATION = "logging.googleapis.com/sourceLocation"
  }

  private data class LogEntryPayload(
    val severity: LogSeverity,
    val timestamp: Instant,
    val textPayload: String,
    val sourceLocation: LogEntrySourceLocation,
  ) {
    fun toJsonObject(): JsonObject {
      return JsonObject().apply {
        addProperty(LogFields.SEVERITY, severity.name)
        addProperty(LogFields.TIME, timestamp.toString())
        addProperty(LogFields.MESSAGE, textPayload)
        add(LogFields.SOURCE_LOCATION, sourceLocation.toJsonObject())
      }
    }
  }

  private data class LogEntrySourceLocation(
    val function: String,
    val file: String? = null,
    val line: Long? = null,
  ) {
    fun toJsonObject(): JsonObject {
      return JsonObject().apply {
        addProperty("function", function)
        if (file != null) {
          addProperty("file", file)
        }
        if (line != null) {
          addProperty("line", line)
        }
      }
    }
  }

  override fun format(record: LogRecord): String {
    return record.toEntryPayload().toJsonObject().toString() + System.lineSeparator()
  }

  companion object {
    private fun LogRecord.toEntryPayload(): LogEntryPayload {
      val sourceLocation = LogEntrySourceLocation(function = "$sourceClassName.$sourceMethodName")
      val textPayload =
        if (thrown == null) {
          message
        } else {
          StringWriter().use { stringWriter ->
            PrintWriter(stringWriter).use { printWriter ->
              printWriter.println(message)
              thrown.printStackTrace(printWriter)
            }
            stringWriter.toString()
          }
        }
      return LogEntryPayload(
        level.toSeverity(),
        Instant.ofEpochMilli(millis),
        textPayload,
        sourceLocation,
      )
    }
  }
}

private fun Level.toSeverity(): LogSeverity {
  return when (name) {
    Level.SEVERE.name -> LogSeverity.ERROR
    Level.WARNING.name -> LogSeverity.WARNING
    Level.INFO.name,
    Level.CONFIG.name -> LogSeverity.INFO
    Level.FINE.name,
    Level.FINER.name,
    Level.FINEST.name -> LogSeverity.DEBUG
    else -> error("Unhandled log level $name")
  }
}

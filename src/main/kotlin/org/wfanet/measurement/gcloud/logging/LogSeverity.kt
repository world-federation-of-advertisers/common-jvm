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

/**
 * Severity for a log entry.
 *
 * See https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#logseverity.
 */
enum class LogSeverity(val number: Int) {
  /** The log entry has no assigned severity level. */
  DEFAULT(0),
  /** Debug or trace information. */
  DEBUG(100),
  /** Routine information, such as ongoing status or performance. */
  INFO(200),
  /** Normal but significant events, such as start up, shut down, or a configuration change. */
  NOTICE(300),
  /** Warning events might cause problems. */
  WARNING(400),
  /** Error events are likely to cause problems. */
  ERROR(500),
  /** Critical events cause more severe problems or outages. */
  CRITICAL(600),
  /** A person must take an action immediately. */
  ALERT(700),
  /** One or more systems are unusable. */
  EMERGENCY(800),
}

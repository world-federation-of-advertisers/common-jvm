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

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SessionPoolOptions
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.SpannerOptions
import com.google.cloud.spanner.Statement
import org.wfanet.measurement.common.Instrumentation

/**
 * Convenience function for appending without worrying about whether the last [append] had
 * sufficient whitespace -- this adds a newline before and a space after.
 */
fun Statement.Builder.appendClause(sql: String): Statement.Builder = append("\n$sql ")

/** Convenience function for applying a Mutation to a transaction. */
fun Mutation.bufferTo(transactionContext: AsyncDatabaseClient.TransactionContext) {
  transactionContext.buffer(this)
}

/** Constructs a [Spanner]. */
fun buildSpanner(projectName: String, spannerEmulatorHost: String? = null): Spanner {
  SpannerOptions.enableOpenTelemetryMetrics()
  SpannerOptions.enableOpenTelemetryTraces()
  return SpannerOptions.newBuilder()
    .apply {
      setProjectId(projectName)
      if (!spannerEmulatorHost.isNullOrBlank()) {
        setEmulatorHost(spannerEmulatorHost)
      }
      setSessionPoolOption(SessionPoolOptions.newBuilder().setWarnIfInactiveTransactions().build())
      setOpenTelemetry(Instrumentation.openTelemetry)
    }
    .build()
    .service
}

/**
 * The wrapped cause of this exception if it doesn't have a known [ErrorCode], or `null` otherwise.
 */
val SpannerException.wrappedException: Throwable?
  get() = if (errorCode == ErrorCode.UNKNOWN) cause else null

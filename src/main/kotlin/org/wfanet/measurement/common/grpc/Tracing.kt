package org.wfanet.measurement.common.grpc

import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.TraceId

object Tracing {
  /** Returns the current OpenTelemetry trace ID, or `null` if not available. */
  fun getOtelTraceId(): String? {
    val traceId = Span.current().spanContext.traceId
    return if (traceId == TraceId.getInvalid()) null else traceId
  }
}

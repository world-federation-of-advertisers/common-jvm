// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.junit.Test

class PreconditionsTest {
  @Test
  fun `failGrpc without cause`() {
    try {
      failGrpc(Status.FAILED_PRECONDITION) { "No cause" }
    } catch (e: StatusRuntimeException) {
      assertThat(e.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(e.status.cause).isEqualTo(null)
      assertThat(e.status.description).isEqualTo("No cause")
      assertThat(e.status.cause).isEqualTo(null)
    }
  }

  @Test
  fun `failGrpc with cause`() {
    val cause = StatusException(Status.INTERNAL)
    try {
      failGrpc(Status.FAILED_PRECONDITION, cause) { "Cause of internal error" }
    } catch (e: StatusRuntimeException) {
      assertThat(e.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(e.status.cause.toString()).isEqualTo(StatusException(Status.INTERNAL).toString())
      assertThat(e.status.description).isEqualTo("Cause of internal error")
      assertThat(e.cause.toString()).isEqualTo(StatusException(Status.INTERNAL).toString())
    }
  }
}

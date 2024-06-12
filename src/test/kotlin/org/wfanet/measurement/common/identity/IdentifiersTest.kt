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

package org.wfanet.measurement.common.identity

import com.google.common.truth.Truth.assertThat
import java.io.IOException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class IdentifiersTest {

  @Test
  fun `round trips`() {
    for (i in listOf(1, 10, 64, 1L shl 32, Long.MAX_VALUE)) {
      val externalId1 = ExternalId(i)
      val apiId1 = externalId1.apiId
      val externalId2 = ApiId(apiId1.value).externalId
      val apiId2 = externalId2.apiId

      assertThat(apiId1.value).isEqualTo(apiId2.value)
      assertThat(externalId1.value).isEqualTo(externalId2.value)
    }
  }

  @Test
  fun `0 is invalid for ExternalId`() {
    assertFailsWith<IllegalArgumentException> { ExternalId(0) }
  }

  @Test
  fun `negative numbers are invalid`() {
    assertFailsWith<IllegalArgumentException> { ExternalId(-1) }
  }

  @Test
  fun `invalid ApiId length`() {
    assertFailsWith<IllegalArgumentException> { ApiId("jNQXAC9IVRw") }
    assertFailsWith<IllegalArgumentException> { ApiId("") }
  }

  @Test
  fun `invalid base64 string`() {
    assertFailsWith<IOException> { ApiId("12345678!") }
    assertFailsWith<IOException> { ApiId("012345678") }
  }

  @Test
  fun `0 is invalid for InternalId`() {
    assertFailsWith<IllegalArgumentException> { InternalId(0) }
  }
}

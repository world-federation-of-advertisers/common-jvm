// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toHexString

@RunWith(JUnit4::class)
class HashingTest {
  @Test
  fun `hashSha256 get expected result`() {
    val result = hashSha256(ByteString.copyFromUtf8("foo"))
    assertThat(result.toByteArray().toHexString())
      .isEqualTo("2C26B46B68FFC68FF99B453C1D30413413422D706483BFA0F98A5E886266E7AE")
  }
}

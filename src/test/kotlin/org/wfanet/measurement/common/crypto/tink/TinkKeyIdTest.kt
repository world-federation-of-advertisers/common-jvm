/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TinkKeyIdTest {
  @Test
  fun `TinkKeyId derives deterministic id`() {
    val originPublicKey = TinkPrivateKeyHandle.generateHpke().publicKey
    val originKeyId = TinkKeyId(originPublicKey)

    val loadedPublicKey = TinkPublicKeyHandle(originPublicKey.toByteString())
    val loadedKeyId = TinkKeyId(loadedPublicKey)

    assertThat(loadedKeyId).isEqualTo(originKeyId)
  }
}

/*
 * Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import com.google.gson.JsonPrimitive
import com.google.protobuf.kotlin.toByteStringUtf8
import java.util.Base64
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class Base64Test {
  @Test
  fun `ByteString base64UrlEncode returns encoded string without padding`() {
    assertThat(BYTE_STRING_VALUE.base64UrlEncode()).isEqualTo(STRING_VALUE_BASE64_URL)
  }

  @Test
  fun `ByteArray base64UrlEncode returns encoded string without padding`() {
    assertThat(BYTE_ARRAY_VALUE.base64UrlEncode()).isEqualTo(STRING_VALUE_BASE64_URL)
  }

  @Test
  fun `String base64UrlDecode returns ByteString value`() {
    assertThat(STRING_VALUE_BASE64_URL.base64UrlDecode()).isEqualTo(BYTE_STRING_VALUE)
  }

  @Test
  fun `Long base64UrlEncode round-trips`() {
    assertThat(LONG_VALUE.base64UrlEncode().base64UrlDecode().toLong()).isEqualTo(LONG_VALUE)
  }

  @Test
  fun `base64MimeDecode returns ByteString value`() {
    val encodedJsonString = JsonPrimitive(MIME_ENCODED_BYTE_STRING)
    assertThat(encodedJsonString.base64MimeDecode().toStringUtf8()).isEqualTo(STRING_VALUE)
  }

  companion object {
    private const val STRING_VALUE = "a string"
    private const val STRING_VALUE_BASE64_URL = "YSBzdHJpbmc"
    private val BYTE_STRING_VALUE = STRING_VALUE.toByteStringUtf8()
    private val BYTE_ARRAY_VALUE = STRING_VALUE.encodeToByteArray()
    private val MIME_ENCODED_BYTE_STRING = Base64.getMimeEncoder().encodeToString(BYTE_ARRAY_VALUE)

    private const val LONG_VALUE = -5260288547287550606L // Hex: B6FFB36FBADEB572
  }
}

/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto.tink

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.size

@RunWith(JUnit4::class)
class StreamingEncryptionTest {
  @Test
  fun `encryptChunked returns chunked ciphertext`() {
    val plaintext =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod".toByteStringUtf8()
    val associatedData = "foo".toByteStringUtf8()
    val chunkSizeBytes = 8
    val streamingAead = keyHandle.getPrimitive(StreamingAead::class.java)

    val ciphertextChunks =
      streamingAead.encryptChunked(plaintext, chunkSizeBytes, associatedData).toList()

    ciphertextChunks.forEachIndexed { index, chunk ->
      if (index == ciphertextChunks.size - 1) {
        // Last chunk can be smaller
        assertThat(chunk.size).isAtMost(chunkSizeBytes)
      } else {
        assertThat(chunk.size).isEqualTo(chunkSizeBytes)
      }
    }

    val inputChannel =
      Channels.newChannel(ByteArrayInputStream(ciphertextChunks.flatten().toByteArray()))
    val outputBuffer = ByteBuffer.allocate(plaintext.size)
    streamingAead.newDecryptingChannel(inputChannel, associatedData.toByteArray()).use {
      while (outputBuffer.hasRemaining()) {
        it.read(outputBuffer)
      }
    }

    assertThat(ByteString.copyFrom(outputBuffer.flip())).isEqualTo(plaintext)
  }

  companion object {
    init {
      StreamingAeadConfig.register()
    }

    private val KEY_TEMPLATE = KeyTemplates.get("AES256_GCM_HKDF_1MB")
    private val keyHandle = KeysetHandle.generateNew(KEY_TEMPLATE)
  }
}

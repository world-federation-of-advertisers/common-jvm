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

package org.wfanet.measurement.storage.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.read

class InMemoryStorageClientTest : AbstractStorageClientTest<InMemoryStorageClient>() {
  @Before
  fun initStorageClient() {
    storageClient = InMemoryStorageClient()
  }

  @Test
  fun contents() = runBlocking {
    val client = InMemoryStorageClient()
    assertThat(client.contents).isEmpty()

    val blobKey = "some-blob-key"
    val contents = "some-contents".toByteStringUtf8()

    client.writeBlob(blobKey, flowOf(contents))

    assertThat(client.contents.mapValues { it.value.read().flatten() })
      .containsExactly(blobKey, contents)

    client.getBlob(blobKey)?.delete()

    assertThat(client.contents).isEmpty()
  }
}

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

package org.wfanet.measurement.storage.testing

import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat

abstract class AbstractConditionalOperationStorageClientTest<
  T : ConditionalOperationStorageClient
> : AbstractStorageClientTest<T>() {

  @Test
  fun `writeBlobIfUnchanged overwrites existing blob`(): Unit = runBlocking {
    val blobKey = "replacable-blob"
    val blob = storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())

    val replacedBlob = storageClient.writeBlobIfUnchanged(blob, flowOf(testBlobContent))

    assertThat(replacedBlob).contentEqualTo(testBlobContent)
    assertThat(checkNotNull(storageClient.getBlob(blobKey))).contentEqualTo(testBlobContent)
  }

  @Test
  fun `writeBlobIfUnchanged throws error when blob contents changed`(): Unit = runBlocking {
    val blobKey = "blob"
    val blob = storageClient.writeBlob(blobKey, "initial content".toByteStringUtf8())
    storageClient.writeBlob(blobKey, "other content".toByteStringUtf8())

    assertFailsWith<BlobChangedException> {
      storageClient.writeBlobIfUnchanged(blob, flowOf(testBlobContent))
    }
  }
}

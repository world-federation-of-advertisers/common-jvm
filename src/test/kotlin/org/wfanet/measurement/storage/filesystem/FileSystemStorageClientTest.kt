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

package org.wfanet.measurement.storage.filesystem

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.storage.testing.AbstractConditionalOperationStorageClientTest

@RunWith(JUnit4::class)
class FileSystemStorageClientTest :
  AbstractConditionalOperationStorageClientTest<FileSystemStorageClient>() {
  @Rule @JvmField val tempDirectory = TemporaryFolder()

  @Before
  fun initClient() {
    storageClient = FileSystemStorageClient(tempDirectory.root)
  }

  @Test
  fun `writeBlob writes blob to file in subdirectory`() {
    val content = "Lorem ipsum dolor sit amet".toByteStringUtf8()
    val blobKey = "a/b/c/file.txt"

    runBlocking { storageClient.writeBlob(blobKey, content) }

    val relativePath = Paths.get("a", "b", "c", "file.txt")
    val file = tempDirectory.root.toPath().resolve(relativePath).toFile()
    assertThat(file.exists()).isTrue()
    assertThat(file.readByteString()).isEqualTo(content)
  }

  @Test
  fun `writeBlobIfNotFound does not expose staging file`(): Unit = runBlocking {
    val blobKey = "path/blob"
    val contentStarted = CompletableDeferred<Unit>()
    val finishContent = CompletableDeferred<Unit>()
    val write = async {
      storageClient.writeBlobIfNotFound(
        blobKey,
        flow {
          emit("first".toByteStringUtf8())
          contentStarted.complete(Unit)
          finishContent.await()
          emit("second".toByteStringUtf8())
        },
      )
    }

    contentStarted.await()
    try {
      assertThat(storageClient.listBlobs().toList()).isEmpty()
      assertThat(storageClient.listBlobKeysAndPrefixes("path/").toList()).isEmpty()
      val stagingFile = tempDirectory.root.walkTopDown().single { it.isFile }
      val stagingKey = stagingFile.relativeTo(tempDirectory.root).invariantSeparatorsPath
      assertThat(storageClient.getBlob(stagingKey)).isNull()
      assertThat(storageClient.getFreshnessToken(stagingKey)).isNull()
    } finally {
      finishContent.complete(Unit)
    }

    assertThat(write.await().read().toList().joinToString("") { it.toStringUtf8() })
      .isEqualTo("firstsecond")
    assertThat(tempDirectory.root.walkTopDown().filter { it.isFile }.toList()).hasSize(1)
  }

  @Test
  fun `writeBlobIfUnchanged does not expose staging file`(): Unit = runBlocking {
    val blobKey = "path/blob"
    storageClient.writeBlob(blobKey, "original".toByteStringUtf8())
    val freshnessToken = checkNotNull(storageClient.getFreshnessToken(blobKey))
    val destination = tempDirectory.root.resolve(blobKey)
    val contentStarted = CompletableDeferred<Unit>()
    val finishContent = CompletableDeferred<Unit>()
    val write = async {
      storageClient.writeBlobIfUnchanged(
        blobKey,
        freshnessToken,
        flow {
          emit("replacement".toByteStringUtf8())
          contentStarted.complete(Unit)
          finishContent.await()
        },
      )
    }

    contentStarted.await()
    try {
      assertThat(storageClient.listBlobs().toList().map { it.blobKey }).containsExactly(blobKey)
      assertThat(storageClient.listBlobKeysAndPrefixes("path/").toList()).containsExactly(blobKey)
      val stagingFile = tempDirectory.root.walkTopDown().single { it.isFile && it != destination }
      val stagingKey = stagingFile.relativeTo(tempDirectory.root).invariantSeparatorsPath
      assertThat(storageClient.getBlob(stagingKey)).isNull()
      assertThat(storageClient.getFreshnessToken(stagingKey)).isNull()
    } finally {
      finishContent.complete(Unit)
    }

    assertThat(write.await().read().toList().joinToString("") { it.toStringUtf8() })
      .isEqualTo("replacement")
    assertThat(tempDirectory.root.walkTopDown().filter { it.isFile }.toList()).hasSize(1)
  }

  @Test
  fun `writeBlobIfUnchanged removes staging file when content fails`(): Unit = runBlocking {
    val blobKey = "path/blob"
    storageClient.writeBlob(blobKey, "original".toByteStringUtf8())
    val freshnessToken = checkNotNull(storageClient.getFreshnessToken(blobKey))
    val failingContent = flow {
      emit("replacement".toByteStringUtf8())
      throw IllegalStateException("content failed")
    }

    assertFailsWith<IllegalStateException> {
      storageClient.writeBlobIfUnchanged(blobKey, freshnessToken, failingContent)
    }

    assertThat(checkNotNull(storageClient.getBlob(blobKey)).read().toList().single().toStringUtf8())
      .isEqualTo("original")
    assertThat(tempDirectory.root.walkTopDown().filter { it.isFile }.toList())
      .containsExactly(tempDirectory.root.resolve(blobKey))
  }

  @Test
  fun `writeBlob rejects reserved staging path`(): Unit = runBlocking {
    assertFailsWith<IllegalArgumentException> {
      storageClient.writeBlob("path/.wfa-storage-client-staging/blob", "content".toByteStringUtf8())
    }
  }
}

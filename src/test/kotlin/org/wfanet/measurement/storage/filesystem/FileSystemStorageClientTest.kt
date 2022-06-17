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
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest

@RunWith(JUnit4::class)
class FileSystemStorageClientTest : AbstractStorageClientTest<FileSystemStorageClient>() {
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
}

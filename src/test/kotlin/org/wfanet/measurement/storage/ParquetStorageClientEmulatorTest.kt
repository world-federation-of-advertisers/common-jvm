/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.storage

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

/**
 * End-to-end tests for [ParquetStorageClient] over the `gs://` scheme, backed by the GCS
 * storage-testbench emulator (Docker). Exercises the real Hadoop GCS connector path: ranged network
 * reads of the parquet footer + row groups and network writes via `HadoopOutputFile` — none of
 * which the local-filesystem unit tests cover. PME is transport-agnostic (parquet-mr's native key
 * tools) and is covered by [ParquetStorageClientTest].
 */
@RunWith(JUnit4::class)
class ParquetStorageClientEmulatorTest {
  @Before
  fun createBucket() {
    storageEmulator.createBucket(BUCKET)
  }

  @After
  fun deleteBucket() {
    storageEmulator.deleteBucketRecursive(BUCKET)
  }

  /** Hadoop [Configuration] pointing the GCS connector at the emulator. */
  private fun gcsConfiguration(): Configuration =
    Configuration().apply {
      set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      set("fs.gs.project.id", "fake-project")
      // No credentials against the local emulator. Service-account auth is
      // enabled by default and is tried before null credentials, so it must be
      // disabled explicitly; otherwise the connector reaches for the GCE
      // metadata server, which fails off-GCE (e.g. in CI).
      setBoolean("fs.gs.auth.service.account.enable", false)
      setBoolean("fs.gs.auth.null.enable", true)
      // Route the JSON API at the emulator; default service path "storage/v1/"
      // is appended by the connector.
      set("fs.gs.storage.root.url", "${storageEmulator.storage.options.host}/")
    }

  private fun newClient(): ParquetStorageClient =
    ParquetStorageClient(gcsConfiguration(), Path("gs://$BUCKET/"))

  private fun rowOf(id: Long, name: String): ParquetRow = parquetRow {
    columns.put("id", parquetValue { int64Value = id })
    columns.put("name", parquetValue { stringValue = name })
  }

  @Test
  fun `writeBlob then read round-trips over gs scheme`(): Unit = runBlocking {
    val client = newClient()
    val row = rowOf(11L, "ada")

    client.writeBlob("dir/data.parquet", flowOf(row.toByteString()))

    val blob = client.getBlob("dir/data.parquet")!!
    assertThat(blob.read().toList().map { ParquetRow.parseFrom(it) }).containsExactly(row)
    val typedRow = blob.readRows().toList().single()
    assertThat(typedRow.getValue("id").int64Value).isEqualTo(11L)
    assertThat(typedRow.getValue("name").stringValue).isEqualTo("ada")
  }

  @Test
  fun `getBlob returns null for missing gs key`(): Unit = runBlocking {
    assertThat(newClient().getBlob("nope.parquet")).isNull()
  }

  @Test
  fun `listBlobs over gs scheme returns written keys`(): Unit = runBlocking {
    val client = newClient()
    client.writeBlob("a.parquet", flowOf(rowOf(1L, "x").toByteString()))
    client.writeBlob("sub/b.parquet", flowOf(rowOf(2L, "y").toByteString()))

    val keys = client.listBlobs().toList().map { it.blobKey }.toSet()

    assertThat(keys).containsExactly("a.parquet", "sub/b.parquet")
  }

  companion object {
    private const val BUCKET = "parquet-test-bucket"

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()
  }
}

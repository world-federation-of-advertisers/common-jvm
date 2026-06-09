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
import com.google.protobuf.ByteString
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.ComplexMessage
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.storage.testing.ParquetTestMessage

@RunWith(JUnit4::class)
class ParquetStorageClientTest {

  // === Test fixtures ===

  /** Parquet schema covering every primitive type the projector supports. */
  private val richSchema: MessageType =
    MessageTypeParser.parseMessageType(
      """
      message RichRow {
        required int32 i32;
        required int64 i64;
        required float f32;
        required double f64;
        required boolean flag;
        required binary str (UTF8);
        required binary data;
        required int32 color_num;
        required binary color_name (UTF8);
        required binary nested_str (UTF8);
        required int64 nested_number;
        required binary nested_inner_label (UTF8);
        optional binary maybe (UTF8);
        repeated int64 multi;
      }
      """
        .trimIndent()
    )

  /**
   * Writes a parquet file to a temp path with the given schema, footer
   * key/value metadata, and rows produced by the supplied [populate]
   * lambda. Returns the bytes.
   */
  private fun writeParquet(
    schema: MessageType,
    metadata: Map<String, String> = emptyMap(),
    populate: (SimpleGroupFactory, ParquetWriter<Group>) -> Unit,
  ): ByteArray {
    val tempPath = Files.createTempFile("parquet_storage_client_test", ".parquet")
    Files.delete(tempPath) // parquet refuses to overwrite an existing path
    return try {
      val hadoopPath = org.apache.hadoop.fs.Path(tempPath.toUri())
      val writer =
        ExampleParquetWriter.builder(hadoopPath)
          .withType(schema)
          .withExtraMetaData(metadata)
          .build()
      val factory = SimpleGroupFactory(schema)
      populate(factory, writer)
      writer.close()
      Files.readAllBytes(tempPath)
    } finally {
      Files.deleteIfExists(tempPath)
    }
  }

  /** Convenience for tests that only need a single-column DOUBLE parquet. */
  private fun writeTinyParquet(
    metadata: Map<String, String> = emptyMap(),
    scores: List<Double> = listOf(1.5, 2.5, 3.5),
  ): ByteArray {
    val schema =
      MessageTypeParser.parseMessageType(
        """
        message TestImpression {
          required double score;
        }
        """
          .trimIndent()
      )
    return writeParquet(schema, metadata) { factory, writer ->
      scores.forEach { score -> writer.write(factory.newGroup().append("score", score)) }
    }
  }

  /**
   * Storage client wrapping an [InMemoryStorageClient] that exposes a
   * counter for how many times the wrapped blob's `read()` is invoked.
   * Used to verify that [ParquetStorageClient]'s materialisation caching
   * limits underlying reads to one per blob across the three read methods.
   */
  private class ReadCountingStorageClient(bytes: ByteArray) : StorageClient {
    private val backing = InMemoryStorageClient()
    var readCount: Int = 0
      private set

    init {
      runBlocking { backing.writeBlob(BLOB_KEY, flowOf(ByteString.copyFrom(bytes))) }
    }

    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
      backing.writeBlob(blobKey, content)

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
      val b = backing.getBlob(blobKey) ?: return null
      return CountingBlob(b)
    }

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      backing.listBlobs(prefix)

    private inner class CountingBlob(private val delegate: StorageClient.Blob) :
      StorageClient.Blob by delegate {
      override val storageClient: StorageClient
        get() = this@ReadCountingStorageClient

      override fun read(): Flow<ByteString> {
        readCount++
        return delegate.read()
      }
    }

    companion object {
      const val BLOB_KEY = "test.parquet"
    }
  }

  /** Pre-built rich-schema parquet with two rows; row 1 has maybe + multi=[10,20], row 2 empties them. */
  private fun richParquetBytes(metadata: Map<String, String> = emptyMap()): ByteArray =
    writeParquet(richSchema, metadata) { factory, writer ->
      val row1 =
        factory
          .newGroup()
          .append("i32", 7)
          .append("i64", 700L)
          .append("f32", 1.25f)
          .append("f64", 2.5)
          .append("flag", true)
          .append("str", "hello")
          .append("data", Binary.fromConstantByteArray(byteArrayOf(0x01, 0x02, 0x03)))
          .append("color_num", 1) // RED
          .append("color_name", "GREEN")
          .append("nested_str", "n-str-1")
          .append("nested_number", 111L)
          .append("nested_inner_label", "inner-1")
          .append("maybe", "present")
          .append("multi", 10L)
          .append("multi", 20L)
      writer.write(row1)
      val row2 =
        factory
          .newGroup()
          .append("i32", -3)
          .append("i64", -300L)
          .append("f32", -1.5f)
          .append("f64", -2.5)
          .append("flag", false)
          .append("str", "")
          .append("data", Binary.EMPTY)
          .append("color_num", 3) // BLUE
          .append("color_name", "BLUE")
          .append("nested_str", "")
          .append("nested_number", 0L)
          .append("nested_inner_label", "")
      // No maybe (OPTIONAL missing). No multi (REPEATED count=0).
      writer.write(row2)
    }

  // === Happy-path tests covering every primitive type ===

  @Test
  fun `projects all primitive types`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "all",
            ParquetTestMessage.getDescriptor(),
            mapOf(
              "i32" to "i32",
              "i64" to "i64",
              "f32" to "f32",
              "f64" to "f64",
              "flag" to "flag",
              "str" to "str",
              "data" to "data",
            ),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("all").toList()

    assertThat(rows).hasSize(2)
    val r1 = rows[0]
    val desc = ParquetTestMessage.getDescriptor()
    assertThat(r1.getField(desc.findFieldByName("i32"))).isEqualTo(7)
    assertThat(r1.getField(desc.findFieldByName("i64"))).isEqualTo(700L)
    assertThat(r1.getField(desc.findFieldByName("f32"))).isEqualTo(1.25f)
    assertThat(r1.getField(desc.findFieldByName("f64"))).isEqualTo(2.5)
    assertThat(r1.getField(desc.findFieldByName("flag"))).isEqualTo(true)
    assertThat(r1.getField(desc.findFieldByName("str"))).isEqualTo("hello")
    assertThat(r1.getField(desc.findFieldByName("data")))
      .isEqualTo(ByteString.copyFrom(byteArrayOf(0x01, 0x02, 0x03)))
  }

  // === Enum tests ===

  @Test
  fun `projects enum from parquet INT32 column by number`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("color_num" to "color_num"),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()

    val color = ParquetTestMessage.getDescriptor().findFieldByName("color_num")
    assertThat((rows[0].getField(color) as com.google.protobuf.Descriptors.EnumValueDescriptor).number)
      .isEqualTo(ParquetTestMessage.Color.RED.number)
    assertThat((rows[1].getField(color) as com.google.protobuf.Descriptors.EnumValueDescriptor).number)
      .isEqualTo(ParquetTestMessage.Color.BLUE.number)
  }

  @Test
  fun `projects enum from parquet BINARY UTF8 column by name`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("color_name" to "color_name"),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()

    val color = ParquetTestMessage.getDescriptor().findFieldByName("color_name")
    assertThat((rows[0].getField(color) as com.google.protobuf.Descriptors.EnumValueDescriptor).name)
      .isEqualTo("GREEN")
    assertThat((rows[1].getField(color) as com.google.protobuf.Descriptors.EnumValueDescriptor).name)
      .isEqualTo("BLUE")
  }

  @Test
  fun `enum from INT32 with unknown number throws at read`() = runBlocking {
    // Build a one-row parquet with color_num = 99 (not a valid Color number).
    val schema =
      MessageTypeParser.parseMessageType(
        "message R { required int32 color_num; }"
      )
    val bytes =
      writeParquet(schema) { factory, writer ->
        writer.write(factory.newGroup().append("color_num", 99))
      }
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("color_num" to "color_num"),
          )
        ),
      )
    val ex =
      assertFailsWith<IllegalStateException> {
        client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
      }
    assertThat(ex.message).contains("number=99")
  }

  @Test
  fun `enum from string with unknown name throws at read`() = runBlocking {
    val schema =
      MessageTypeParser.parseMessageType(
        "message R { required binary color_name (UTF8); }"
      )
    val bytes =
      writeParquet(schema) { factory, writer ->
        writer.write(factory.newGroup().append("color_name", "PURPLE"))
      }
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("color_name" to "color_name"),
          )
        ),
      )
    val ex =
      assertFailsWith<IllegalStateException> {
        client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
      }
    assertThat(ex.message).contains("name='PURPLE'")
  }

  // === Nested-path tests ===

  @Test
  fun `projects 1-level nested path`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("nested.str" to "nested_str"),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
    // Round-trip DynamicMessage → typed for ergonomic nested field assertions.
    val typed = ParquetTestMessage.parseFrom(rows[0].toByteString())
    assertThat(typed.nested.str).isEqualTo("n-str-1")
  }

  @Test
  fun `projects 2-level nested path`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("nested.inner.label" to "nested_inner_label"),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
    val typed = ParquetTestMessage.parseFrom(rows[0].toByteString())
    assertThat(typed.nested.inner.label).isEqualTo("inner-1")
  }

  @Test
  fun `multiple mappings into same nested message share parent builder`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf(
              "nested.str" to "nested_str",
              "nested.number" to "nested_number",
              "nested.inner.label" to "nested_inner_label",
            ),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
    val typed = ParquetTestMessage.parseFrom(rows[0].toByteString())
    assertThat(typed.nested.str).isEqualTo("n-str-1")
    assertThat(typed.nested.number).isEqualTo(111L)
    assertThat(typed.nested.inner.label).isEqualTo("inner-1")
  }

  // === OPTIONAL / REPEATED parquet handling ===

  @Test
  fun `OPTIONAL parquet column missing leaves proto field at default`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("maybe" to "maybe"),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()

    val maybeField = ParquetTestMessage.getDescriptor().findFieldByName("maybe")
    // Row 1 wrote "present"; row 2 did not write maybe at all.
    assertThat(rows[0].getField(maybeField)).isEqualTo("present")
    assertThat(rows[1].getField(maybeField)).isEqualTo("")
  }

  @Test
  fun `REPEATED parquet column with count gt 1 throws at read`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            // multi is REPEATED in the parquet schema; row 1 has 2 values; row 2 has 0.
            // Mapping to non-repeated proto target i64 should throw on row 1.
            mapOf("i64" to "multi"),
          )
        ),
      )
    val ex =
      assertFailsWith<IllegalStateException> {
        client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
      }
    assertThat(ex.message).contains("REPEATED parquet column")
    assertThat(ex.message).contains("2 values")
  }

  // === Multi-projection in one pass ===

  @Test
  fun `readProjectedMessages emits one map per row with every projection`() = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "a",
            ParquetTestMessage.getDescriptor(),
            mapOf("i32" to "i32"),
          ),
          ParquetStorageClient.Projection(
            "b",
            ParquetTestMessage.getDescriptor(),
            mapOf("str" to "str"),
          ),
        ),
      )
    val rows =
      client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readProjectedMessages().toList()

    assertThat(rows).hasSize(2)
    rows.forEach { row -> assertThat(row.keys).containsExactly("a", "b") }
    val i32 = ParquetTestMessage.getDescriptor().findFieldByName("i32")
    val str = ParquetTestMessage.getDescriptor().findFieldByName("str")
    assertThat(rows[0].getValue("a").getField(i32)).isEqualTo(7)
    assertThat(rows[0].getValue("b").getField(str)).isEqualTo("hello")
  }

  // === Per-name isolation ===

  @Test
  fun `readMessages on good projection succeeds even when sibling has bad source column`():
    Unit = runBlocking {
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "good",
            ParquetTestMessage.getDescriptor(),
            mapOf("i32" to "i32"),
          ),
          // bad projection: maps to non-existent parquet column.
          ParquetStorageClient.Projection(
            "bad",
            ParquetTestMessage.getDescriptor(),
            mapOf("i32" to "definitely_not_a_column"),
          ),
        ),
      )
    val blob = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!

    // good works fine — bind only runs for the requested projection.
    val rows = blob.readMessages("good").toList()
    assertThat(rows).hasSize(2)

    // bad throws on its own.
    assertFailsWith<IllegalStateException> { blob.readMessages("bad").toList() }
  }

  // === Caching / read-count ===

  @Test
  fun `materialisation is cached across read methods on one blob`() = runBlocking {
    val bytes = richParquetBytes(metadata = mapOf("k" to "v"))
    val counter = ReadCountingStorageClient(bytes)
    val client =
      ParquetStorageClient(
        counter,
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("i32" to "i32"),
          )
        ),
      )
    val blob = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!

    blob.readKeyValueMetadata()
    blob.readMessages("p").toList()
    blob.readProjectedMessages().toList()

    assertThat(counter.readCount).isEqualTo(1)
  }

  // === Metadata ===

  @Test
  fun `readKeyValueMetadata returns parquet footer metadata`() = runBlocking {
    val bytes = writeTinyParquet(mapOf("encrypted_dek" to "abc123", "ref_id" to "xyz"))
    val client = ParquetStorageClient(ReadCountingStorageClient(bytes), emptyList())
    val blob = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!

    val metadata = blob.readKeyValueMetadata()

    assertThat(metadata).containsEntry("encrypted_dek", "abc123")
    assertThat(metadata).containsEntry("ref_id", "xyz")
  }

  // === Column-pushdown sanity check ===

  @Test
  fun `extra unmapped columns do not break read (column pushdown sanity)`() = runBlocking {
    // Wide schema; only one column is mapped. Reads must succeed regardless
    // of the unmapped columns (and in production parquet pushdown skips
    // decoding them — verified indirectly: the read works on a wide row).
    val bytes = richParquetBytes()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("i32" to "i32"),
          )
        ),
      )
    val rows = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
    assertThat(rows).hasSize(2)
  }

  // === Read-time error: source column not in parquet schema ===

  @Test
  fun `source column not in parquet schema throws at first row`() = runBlocking {
    val bytes = writeTinyParquet()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "p",
            ParquetTestMessage.getDescriptor(),
            mapOf("i32" to "definitely_not_a_column"),
          )
        ),
      )
    val ex =
      assertFailsWith<IllegalStateException> {
        client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!.readMessages("p").toList()
      }
    assertThat(ex.message).contains("not in the row schema")
  }

  // === Construction-time validation ===

  @Test
  fun `construction throws on duplicate projection names`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        ParquetStorageClient(
          InMemoryStorageClient(),
          listOf(
            ParquetStorageClient.Projection(
              "dupe",
              ParquetTestMessage.getDescriptor(),
              emptyMap(),
            ),
            ParquetStorageClient.Projection(
              "dupe",
              ParquetTestMessage.getDescriptor(),
              emptyMap(),
            ),
          ),
        )
      }
    assertThat(ex.message).contains("unique")
  }

  @Test
  fun `construction throws on target path referencing non-existent field`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        ParquetStorageClient(
          InMemoryStorageClient(),
          listOf(
            ParquetStorageClient.Projection(
              "p",
              ParquetTestMessage.getDescriptor(),
              fieldMapping = mapOf("does_not_exist" to "src"),
            )
          ),
        )
      }
    assertThat(ex.message).contains("non-existent")
  }

  @Test
  fun `construction throws on repeated target field`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        ParquetStorageClient(
          InMemoryStorageClient(),
          listOf(
            ParquetStorageClient.Projection(
              "p",
              ComplexMessage.getDescriptor(),
              // ComplexMessage.field1 is `repeated int32`
              fieldMapping = mapOf("field1" to "src"),
            )
          ),
        )
      }
    assertThat(ex.message).contains("repeated")
  }

  @Test
  fun `construction throws on navigating through non-message field`() {
    val ex =
      assertFailsWith<IllegalArgumentException> {
        ParquetStorageClient(
          InMemoryStorageClient(),
          listOf(
            ParquetStorageClient.Projection(
              "p",
              ParquetTestMessage.getDescriptor(),
              // i32 is INT, not MESSAGE — can't navigate through it.
              fieldMapping = mapOf("i32.something" to "src"),
            )
          ),
        )
      }
    assertThat(ex.message).contains("non-message field")
  }

  @Test
  fun `unknown projection name throws IllegalArgumentException at read`() = runBlocking {
    val bytes = writeTinyParquet()
    val client =
      ParquetStorageClient(
        ReadCountingStorageClient(bytes),
        listOf(
          ParquetStorageClient.Projection(
            "configured",
            ComplexMessage.getDescriptor(),
            mapOf("field3" to "score"),
          )
        ),
      )
    val blob = client.getBlob(ReadCountingStorageClient.BLOB_KEY)!!

    val ex = assertFailsWith<IllegalArgumentException> { blob.readMessages("unknown").toList() }
    assertThat(ex.message).contains("No Projection named")
  }
}

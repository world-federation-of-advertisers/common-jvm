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
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.date
import java.io.File
import java.security.SecureRandom
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient

// TODO(world-federation-of-advertisers/cross-media-measurement#3955): Extend the
// shared AbstractStorageClientTest once it is content-parameterized. This client's
// read()/writeBlob speak a ParquetRow proto codec, so it can't use the base
// class's opaque-byte fixtures; until then the writeBlob -> getBlob -> read()
// contract is covered by the dedicated round-trip tests below.
@RunWith(JUnit4::class)
class ParquetStorageClientTest {
  @Rule @JvmField val tempDir = TemporaryFolder()

  /**
   * Converts a `readRows()` row back to native Kotlin values so the assertions below can stay
   * value-oriented. This is the inverse of the client's native -> [ParquetValue] mapping;
   * `KIND_NOT_SET` -> `null`.
   */
  private fun Map<String, ParquetValue>.toNative(): Map<String, Any?> = mapValues {
    it.value.toNative()
  }

  private fun ParquetValue.toNative(): Any? =
    when (kindCase) {
      ParquetValue.KindCase.INT32_VALUE -> int32Value
      ParquetValue.KindCase.INT64_VALUE -> int64Value
      ParquetValue.KindCase.FLOAT_VALUE -> floatValue
      ParquetValue.KindCase.DOUBLE_VALUE -> doubleValue
      ParquetValue.KindCase.BOOL_VALUE -> boolValue
      ParquetValue.KindCase.STRING_VALUE -> stringValue
      ParquetValue.KindCase.BYTES_VALUE -> bytesValue
      ParquetValue.KindCase.UINT32_VALUE -> uint32Value.toUInt()
      ParquetValue.KindCase.UINT64_VALUE -> uint64Value.toULong()
      ParquetValue.KindCase.TIMESTAMP_VALUE ->
        Instant.ofEpochSecond(timestampValue.seconds, timestampValue.nanos.toLong())
      ParquetValue.KindCase.DATE_VALUE ->
        LocalDate.of(dateValue.year, dateValue.month, dateValue.day)
      ParquetValue.KindCase.KIND_NOT_SET -> null
    }

  private fun newClient(): ParquetStorageClient =
    ParquetStorageClient(Configuration(), Path(tempDir.root.absolutePath))

  /** A [ParquetStorageClient] wired to parquet-mr's native PME via [kms]. */
  private fun newEncryptingClient(
    conf: Configuration,
    kms: FakeKmsClient,
    keyMapping: Map<String, String> = emptyMap(),
  ): ParquetStorageClient =
    ParquetStorageClient(
      conf,
      Path(tempDir.root.absolutePath),
      encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kms }, keyMapping = keyMapping),
    )

  /** A [FakeKmsClient] holding a fresh AES-256-GCM Aead for each of [kekUris]. */
  private fun fakeKms(vararg kekUris: String): FakeKmsClient =
    FakeKmsClient().also { client ->
      for (uri in kekUris) {
        val aead =
          KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM")).getPrimitive(Aead::class.java)
        client.setAead(uri, aead)
      }
    }

  /** Serialized sample [ParquetRow] used by the encryption round-trip tests. */
  private fun encRow(): ByteString =
    parquetRow {
        columns.put("id", parquetValue { int64Value = 7L })
        columns.put("name", parquetValue { stringValue = "zoe" })
      }
      .toByteString()

  // ===== Plaintext reads =====

  @Test
  fun `readRows on plaintext blob returns native-typed values keyed by column name`(): Unit =
    runBlocking {
      val key = plaintextSample("data.parquet")

      val rows = newClient().getBlob(key)!!.readRows().toList().map { it.toNative() }

      assertThat(rows).hasSize(3)
      assertThat(rows[0]).containsExactly("id", 1L, "name", "alice", "data", BYTES_AB)
      assertThat(rows[1]).containsExactly("id", 2L, "name", "bob", "data", BYTES_CD)
      assertThat(rows[2]).containsExactly("id", 3L, "name", "carol", "data", BYTES_EF)
    }

  @Test
  fun `readSchema returns column name to value kind`(): Unit = runBlocking {
    val key = plaintextSample("data.parquet")

    val schema = newClient().getBlob(key)!!.readSchema()

    assertThat(schema)
      .containsExactly(
        "id",
        ParquetValue.KindCase.INT64_VALUE,
        "name",
        ParquetValue.KindCase.STRING_VALUE,
        "data",
        ParquetValue.KindCase.BYTES_VALUE,
      )
  }

  @Test
  fun `readSchema returns declared schema for a zero-row file`(): Unit = runBlocking {
    val key = writeParquetBlob("empty.parquet", SAMPLE_SCHEMA, emptyList(), null, emptyMap(), null)

    assertThat(newClient().getBlob(key)!!.readSchema())
      .containsExactly(
        "id",
        ParquetValue.KindCase.INT64_VALUE,
        "name",
        ParquetValue.KindCase.STRING_VALUE,
        "data",
        ParquetValue.KindCase.BYTES_VALUE,
      )
  }

  @Test
  fun `readKeyValueMetadata returns plaintext footer entries`(): Unit = runBlocking {
    val key = plaintextSample("data.parquet", metadata = mapOf("foo" to "bar"))

    assertThat(newClient().getBlob(key)!!.readKeyValueMetadata()).containsAtLeast("foo", "bar")
  }

  @Test
  fun `writeBlob embeds keyValueMetadata into the footer`(): Unit = runBlocking {
    val client = newClient()

    client.writeBlob(
      "meta.parquet",
      flowOf(encRow()),
      mapOf("edpa.kek_uri" to "fake-kms://kek", "shard" to "3"),
    )

    assertThat(client.getBlob("meta.parquet")!!.readKeyValueMetadata())
      .containsAtLeast("edpa.kek_uri", "fake-kms://kek", "shard", "3")
  }

  @Test
  fun `writeBlob embeds keyValueMetadata on an empty zero-row blob`(): Unit = runBlocking {
    val client = newClient()

    client.writeBlob("empty-meta.parquet", emptyFlow(), mapOf("foo" to "bar"))

    assertThat(client.getBlob("empty-meta.parquet")!!.readKeyValueMetadata())
      .containsAtLeast("foo", "bar")
  }

  @Test
  fun `getBlob returns null for missing key`(): Unit = runBlocking {
    assertThat(newClient().getBlob("nothing-here.parquet")).isNull()
  }

  @Test
  fun `empty parquet file emits no rows`(): Unit = runBlocking {
    val key = writeParquetBlob("empty.parquet", SAMPLE_SCHEMA, emptyList(), null, emptyMap(), null)

    assertThat(newClient().getBlob(key)!!.readRows().toList()).isEmpty()
  }

  @Test
  fun `listBlobs returns written blobs`(): Unit = runBlocking {
    plaintextSample("a.parquet")
    plaintextSample("b.parquet")

    val keys = newClient().listBlobs().toList().map { it.blobKey }.toSet()

    assertThat(keys).containsExactly("a.parquet", "b.parquet")
  }

  @Test
  fun `listBlobs lists recursively and includes a non-parquet marker`(): Unit = runBlocking {
    // Mirrors the EDP layout: a `done` marker plus parquet files in the
    // directory and a subdirectory. Listing must surface all of them
    // (including the non-parquet marker) so the caller can filter it out.
    File(tempDir.root, "done").writeText("")
    plaintextSample("file1.parquet")
    plaintextSample("file2.parquet")
    File(tempDir.root, "sub").mkdirs()
    plaintextSample("sub/file3.parquet")

    val keys = newClient().listBlobs().toList().map { it.blobKey }.toSet()

    assertThat(keys).containsExactly("done", "file1.parquet", "file2.parquet", "sub/file3.parquet")
  }

  @Test
  fun `listBlobs filters by prefix`(): Unit = runBlocking {
    plaintextSample("a.parquet")
    File(tempDir.root, "sub").mkdirs()
    plaintextSample("sub/b.parquet")

    val keys = newClient().listBlobs("sub/").toList().map { it.blobKey }.toSet()

    assertThat(keys).containsExactly("sub/b.parquet")
  }

  @Test
  fun `listBlobs matches a partial filename prefix within a folder`(): Unit = runBlocking {
    // The directory part ("sub") is listed server-side; the trailing "file"
    // segment is matched client-side, preserving string-prefix semantics.
    File(tempDir.root, "sub").mkdirs()
    plaintextSample("sub/file1.parquet")
    plaintextSample("sub/file2.parquet")
    plaintextSample("sub/other.parquet")

    val keys = newClient().listBlobs("sub/file").toList().map { it.blobKey }.toSet()

    assertThat(keys).containsExactly("sub/file1.parquet", "sub/file2.parquet")
  }

  @Test
  fun `listBlobs with a missing prefix directory returns empty`(): Unit = runBlocking {
    plaintextSample("a.parquet")

    // The scoped list root does not exist; this must yield no blobs, not throw.
    assertThat(newClient().listBlobs("nope/missing").toList()).isEmpty()
  }

  @Test
  fun `concurrent readRows on the same blob each return the full data`(): Unit = runBlocking {
    // Each readRows() opens its own HadoopInputFile + reader; only the cached
    // FileSystem is shared. Verify concurrent collections on one ParquetBlob all
    // get the complete, correct data.
    val key = plaintextSample("data.parquet") // rows: alice, bob, carol
    val blob = newClient().getBlob(key)!!

    val results =
      (1..8)
        .map {
          async(Dispatchers.Default) {
            blob.readRows().toList().map { row -> row.toNative()["name"] }
          }
        }
        .awaitAll()

    assertThat(results).hasSize(8)
    results.forEach { names ->
      assertThat(names).containsExactly("alice", "bob", "carol").inOrder()
    }
  }

  @Test
  fun `blob exposes size and timestamps`(): Unit = runBlocking {
    val key = plaintextSample("data.parquet")

    val blob = newClient().getBlob(key)!!

    assertThat(blob.size).isGreaterThan(0L)
    assertThat(blob.createTime).isNotNull()
    assertThat(blob.updateTime).isNotNull()
  }

  // ===== StorageClient codec round trip (writeBlob <-> read) =====

  @Test
  fun `read after writeBlob round-trips all supported ParquetValue kinds`(): Unit = runBlocking {
    val sampleTimestamp =
      // Micro-aligned nanos: the canonical write precision is MICROS, so a
      // non-micro nanos value would not survive the round trip.
      timestamp {
        seconds = 1_700_000_000L
        nanos = 123_456_000
      }
    val sampleDate = date {
      year = 2026
      month = 6
      day = 10
    }
    val row = parquetRow {
      columns.put("i32", parquetValue { int32Value = 7 })
      columns.put("i64", parquetValue { int64Value = 8L })
      columns.put("f32", parquetValue { floatValue = 1.5f })
      columns.put("f64", parquetValue { doubleValue = 2.5 })
      columns.put("flag", parquetValue { boolValue = true })
      columns.put("s", parquetValue { stringValue = "hi" })
      columns.put(
        "b",
        parquetValue { bytesValue = ByteString.copyFrom(byteArrayOf(0x01, 0x02, 0x03)) },
      )
      columns.put("t", parquetValue { timestampValue = sampleTimestamp })
      columns.put("d", parquetValue { dateValue = sampleDate })
    }

    val client = newClient()
    client.writeBlob("rt.parquet", flowOf(row.toByteString()))
    val blob = client.getBlob("rt.parquet")!!

    // read() round-trips the ParquetRow proto.
    val readBack = blob.read().toList().map { ParquetRow.parseFrom(it) }
    assertThat(readBack).containsExactly(row)

    // readRows() projects the same row into native types.
    val native = blob.readRows().toList().single().toNative()
    assertThat(native["i32"]).isEqualTo(7)
    assertThat(native["i64"]).isEqualTo(8L)
    assertThat(native["f32"]).isEqualTo(1.5f)
    assertThat(native["f64"]).isEqualTo(2.5)
    assertThat(native["flag"]).isEqualTo(true)
    assertThat(native["s"]).isEqualTo("hi")
    assertThat(native["b"]).isEqualTo(ByteString.copyFrom(byteArrayOf(0x01, 0x02, 0x03)))
    assertThat(native["t"]).isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_000L))
    assertThat(native["d"]).isEqualTo(LocalDate.of(2026, 6, 10))
  }

  @Test
  fun `writeBlob with empty flow writes a readable zero-row blob`(): Unit = runBlocking {
    val client = newClient()
    client.writeBlob("empty-rt.parquet", emptyFlow())

    val blob = client.getBlob("empty-rt.parquet")!!
    assertThat(blob.read().toList()).isEmpty()
    assertThat(blob.readRows().toList()).isEmpty()
  }

  @Test
  fun `writeBlob then read round-trips multiple rows including a NULL value`(): Unit = runBlocking {
    // First row fully populated (schema is derived from it); second row omits
    // the optional column, which round-trips as a KIND_NOT_SET value.
    val row1 = parquetRow {
      columns.put("id", parquetValue { int64Value = 1L })
      columns.put("name", parquetValue { stringValue = "alice" })
    }
    val row2 = parquetRow {
      columns.put("id", parquetValue { int64Value = 2L })
      columns.put("name", ParquetValue.getDefaultInstance()) // explicit NULL
    }

    val client = newClient()
    client.writeBlob("rows.parquet", flowOf(row1.toByteString(), row2.toByteString()))

    val readBack = client.getBlob("rows.parquet")!!.read().toList().map { ParquetRow.parseFrom(it) }
    assertThat(readBack).containsExactly(row1, row2).inOrder()
  }

  @Test
  fun `writeBlob rejects a later row with a column absent from the first row`(): Unit =
    runBlocking {
      val row1 = parquetRow { columns.put("id", parquetValue { int64Value = 1L }) }
      val row2 = parquetRow {
        columns.put("id", parquetValue { int64Value = 2L })
        columns.put("extra", parquetValue { stringValue = "x" })
      }

      assertFailsWith<IllegalArgumentException> {
        newClient().writeBlob("rows.parquet", flowOf(row1.toByteString(), row2.toByteString()))
      }
    }

  @Test
  fun `writeBlob rejects a later row whose column kind differs from the first`(): Unit =
    runBlocking {
      val row1 = parquetRow { columns.put("v", parquetValue { int64Value = 1L }) }
      val row2 = parquetRow { columns.put("v", parquetValue { stringValue = "two" }) }

      assertFailsWith<IllegalArgumentException> {
        newClient().writeBlob("rows.parquet", flowOf(row1.toByteString(), row2.toByteString()))
      }
    }

  @Test
  fun `writeBlob rejects a first row with an unset column`(): Unit = runBlocking {
    // The schema is derived from the first row, so a KIND_NOT_SET value there
    // cannot be typed and must fail clearly (naming the offending column).
    val row = parquetRow {
      columns.put("id", parquetValue { int64Value = 1L })
      columns.put("missing", ParquetValue.getDefaultInstance()) // KIND_NOT_SET
    }

    assertFailsWith<IllegalArgumentException> {
      newClient().writeBlob("kns.parquet", flowOf(row.toByteString()))
    }
  }

  @Test
  fun `writeBlob rounds sub-microsecond timestamps to micros`(): Unit = runBlocking {
    // The codec writes TIMESTAMP(MICROS). Sub-microsecond nanos are rounded to the
    // nearest microsecond (half up) rather than rejected, so an arbitrary parquet
    // timestamp never crashes this code. 999 nanos rounds up to 1 microsecond.
    val row = parquetRow {
      columns.put(
        "t",
        parquetValue {
          timestampValue = timestamp {
            seconds = 1L
            nanos = 999
          }
        },
      )
    }

    val client = newClient()
    client.writeBlob("ts.parquet", flowOf(row.toByteString()))

    val native = client.getBlob("ts.parquet")!!.readRows().toList().single().toNative()
    assertThat(native["t"]).isEqualTo(Instant.ofEpochSecond(1L, 1_000L))
  }

  @Test
  fun `writeBlob failure deletes the partial blob`(): Unit = runBlocking {
    val client = newClient()
    // Row 1 is valid (creates the output file); row 2 fails validation, which
    // must trigger cleanup so no half-written blob is left behind.
    val row1 = parquetRow { columns.put("id", parquetValue { int64Value = 1L }) }
    val row2 = parquetRow {
      columns.put("id", parquetValue { int64Value = 2L })
      columns.put("extra", parquetValue { stringValue = "x" })
    }

    assertFailsWith<IllegalArgumentException> {
      client.writeBlob("partial.parquet", flowOf(row1.toByteString(), row2.toByteString()))
    }

    assertThat(client.getBlob("partial.parquet")).isNull()
  }

  // ===== Primitive-type coverage (reads) =====

  @Test
  fun `readRows decodes unsigned integer logical types without wraparound`(): Unit = runBlocking {
    // Values that overflow signed Int/Long: a UINT_32 of 3 billion and a UINT_64
    // above Long.MAX. Signed decoding would corrupt these into negatives.
    val schema =
      MessageTypeParser.parseMessageType(
        """message Row { required int32 u32 (INTEGER(32,false)); required int64 u64 (INTEGER(64,false)); }"""
      )
    val u32 = 3_000_000_000u
    val u64 = 18_000_000_000_000_000_000uL
    val row =
      groupOf(schema) {
        add("u32", u32.toInt()) // raw bits
        add("u64", u64.toLong()) // raw bits
      }
    val key = writeParquetBlob("uint.parquet", schema, listOf(row), null, emptyMap(), null)

    val map = newClient().getBlob(key)!!.readRows().toList().single().toNative()
    assertThat(map["u32"]).isInstanceOf(UInt::class.javaObjectType)
    assertThat((map["u32"] as UInt).toLong()).isEqualTo(3_000_000_000L)
    assertThat(map["u64"]).isInstanceOf(ULong::class.javaObjectType)
    assertThat((map["u64"] as ULong).toString()).isEqualTo("18000000000000000000")
  }

  @Test
  fun `writeBlob then read round-trips unsigned values`(): Unit = runBlocking {
    val row = parquetRow {
      columns.put("u32", parquetValue { uint32Value = 3_000_000_000u.toInt() })
      columns.put("u64", parquetValue { uint64Value = 18_000_000_000_000_000_000uL.toLong() })
    }

    val client = newClient()
    client.writeBlob("uint-rt.parquet", flowOf(row.toByteString()))

    val readBack =
      client.getBlob("uint-rt.parquet")!!.read().toList().map { ParquetRow.parseFrom(it) }
    assertThat(readBack).containsExactly(row)
    // And the native map exposes them as unsigned Kotlin types.
    val map = client.getBlob("uint-rt.parquet")!!.readRows().toList().single().toNative()
    assertThat((map["u32"] as UInt).toLong()).isEqualTo(3_000_000_000L)
    assertThat((map["u64"] as ULong).toString()).isEqualTo("18000000000000000000")
  }

  @Test
  fun `readRows decodes INT96 to a raw 12-byte ByteString`(): Unit = runBlocking {
    val schema = MessageTypeParser.parseMessageType("""message Row { required int96 ts96; }""")
    val raw = ByteArray(12) { (it + 1).toByte() } // 0x01..0x0C
    val row = groupOf(schema) { add("ts96", Binary.fromConstantByteArray(raw)) }
    val key = writeParquetBlob("int96.parquet", schema, listOf(row), null, emptyMap(), null)

    assertThat(newClient().getBlob(key)!!.readRows().toList().single().toNative()["ts96"])
      .isEqualTo(ByteString.copyFrom(raw))
  }

  @Test
  fun `readRows preserves nanosecond precision for TIMESTAMP NANOS`(): Unit = runBlocking {
    val schema =
      MessageTypeParser.parseMessageType(
        """message Row { required int64 t (TIMESTAMP(NANOS,true)); }"""
      )
    val epochNanos = 1_700_000_000_123_456_789L // sub-microsecond precision
    val row = groupOf(schema) { add("t", epochNanos) }
    val key = writeParquetBlob("ts-nanos.parquet", schema, listOf(row), null, emptyMap(), null)

    assertThat(newClient().getBlob(key)!!.readRows().toList().single().toNative()["t"])
      .isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_789L))
  }

  @Test
  fun `readRows decodes every supported primitive type`(): Unit = runBlocking {
    val schema =
      MessageTypeParser.parseMessageType(
        """
        message AllPrimitives {
          required int32 i32;
          required int64 i64;
          required float f32;
          required double f64;
          required boolean flag;
          required binary str (STRING);
          required binary enm (ENUM);
          required binary raw;
          required fixed_len_byte_array(4) fixed4;
          required int32 d (DATE);
          required int64 t (TIMESTAMP(MILLIS, true));
        }
        """
          .trimIndent()
      )
    val row =
      groupOf(schema) {
        add("i32", 42)
        add("i64", 1234567890123L)
        add("f32", 1.5f)
        add("f64", 2.5)
        add("flag", true)
        add("str", "hello")
        add("enm", "RED")
        add("raw", Binary.fromConstantByteArray(byteArrayOf(0x01, 0x02, 0x03)))
        add("fixed4", Binary.fromConstantByteArray(byteArrayOf(0x0A, 0x0B, 0x0C, 0x0D)))
        add("d", LocalDate.of(2026, 6, 10).toEpochDay().toInt())
        add("t", 1_700_000_000_123L)
      }
    val key = writeParquetBlob("p.parquet", schema, listOf(row), null, emptyMap(), null)

    val map = newClient().getBlob(key)!!.readRows().toList().single().toNative()
    assertThat(map["i32"]).isEqualTo(42)
    assertThat(map["i64"]).isEqualTo(1234567890123L)
    assertThat(map["f32"]).isEqualTo(1.5f)
    assertThat(map["f64"]).isEqualTo(2.5)
    assertThat(map["flag"]).isEqualTo(true)
    assertThat(map["str"]).isEqualTo("hello")
    // ENUM-annotated BINARY must decode to String.
    assertThat(map["enm"]).isEqualTo("RED")
    assertThat(map["raw"]).isEqualTo(ByteString.copyFrom(byteArrayOf(0x01, 0x02, 0x03)))
    assertThat(map["fixed4"]).isEqualTo(ByteString.copyFrom(byteArrayOf(0x0A, 0x0B, 0x0C, 0x0D)))
    // DATE-annotated INT32 -> LocalDate; TIMESTAMP-annotated INT64 -> Instant.
    assertThat(map["d"]).isEqualTo(LocalDate.of(2026, 6, 10))
    assertThat(map["t"]).isEqualTo(Instant.ofEpochMilli(1_700_000_000_123L))
  }

  @Test
  fun `JSON-annotated BINARY decodes to String`(): Unit = runBlocking {
    val schema = MessageTypeParser.parseMessageType("""message Row { required binary j (JSON); }""")
    val row = groupOf(schema) { add("j", """{"k":1}""") }
    val key = writeParquetBlob("json.parquet", schema, listOf(row), null, emptyMap(), null)

    assertThat(newClient().getBlob(key)!!.readRows().toList().single().toNative()["j"])
      .isEqualTo("""{"k":1}""")
  }

  @Test
  fun `BSON-annotated BINARY decodes to ByteString`(): Unit = runBlocking {
    val schema = MessageTypeParser.parseMessageType("""message Row { required binary b (BSON); }""")
    val raw = byteArrayOf(0x11, 0x22, 0x33)
    val row = groupOf(schema) { add("b", Binary.fromConstantByteArray(raw)) }
    val key = writeParquetBlob("bson.parquet", schema, listOf(row), null, emptyMap(), null)

    // BSON is bytes-encoded (not UTF-8 text) -> kept as ByteString.
    assertThat(newClient().getBlob(key)!!.readRows().toList().single().toNative()["b"])
      .isEqualTo(ByteString.copyFrom(raw))
  }

  @Test
  fun `OPTIONAL column absent in a row maps to null`(): Unit = runBlocking {
    val schema =
      MessageTypeParser.parseMessageType(
        """message Row { required int64 id; optional binary maybe (STRING); }"""
      )
    val rowWithoutMaybe = groupOf(schema) { add("id", 1L) }
    val rowWithMaybe =
      groupOf(schema) {
        add("id", 2L)
        add("maybe", "present")
      }
    val key =
      writeParquetBlob(
        "opt.parquet",
        schema,
        listOf(rowWithoutMaybe, rowWithMaybe),
        null,
        emptyMap(),
        null,
      )

    val rows = newClient().getBlob(key)!!.readRows().toList().map { it.toNative() }
    assertThat(rows[0]).containsExactly("id", 1L, "maybe", null)
    assertThat(rows[1]).containsExactly("id", 2L, "maybe", "present")
  }

  @Test
  fun `multi-row-group file reads all rows in order`(): Unit = runBlocking {
    // Tiny row-group size forces multiple row groups; reading across them
    // exercises parquet's seek-back path against the backend stream.
    val schema =
      MessageTypeParser.parseMessageType(
        """message Row { required int64 n; required binary s (STRING); }"""
      )
    val rows =
      (0 until 200).map { i ->
        groupOf(schema) {
          add("n", i.toLong())
          add("s", "row-$i")
        }
      }
    val key = writeParquetBlob("rg.parquet", schema, rows, null, emptyMap(), rowGroupSize = 1024L)

    val outRows = newClient().getBlob(key)!!.readRows().toList().map { it.toNative() }
    assertThat(outRows).hasSize(200)
    assertThat(outRows.first()).containsExactly("n", 0L, "s", "row-0")
    assertThat(outRows.last()).containsExactly("n", 199L, "s", "row-199")
  }

  // ===== PME paths (native parquet-mr key tools, bridged to Tink) =====

  @Test
  fun `writeBlob then readRows round-trips an encrypted blob`(): Unit = runBlocking {
    val kekUri = "fake-kms://uniform-kek"
    // Uniform key: footer + all columns encrypted under one master key.
    val conf = Configuration().apply { set("parquet.encryption.uniform.key", kekUri) }
    val client = newEncryptingClient(conf, fakeKms(kekUri))

    client.writeBlob("enc.parquet", flowOf(encRow()))

    // The same client's conf carries the crypto factory, so it decrypts on read.
    val row = client.getBlob("enc.parquet")!!.readRows().toList().single().toNative()
    assertThat(row["id"]).isEqualTo(7L)
    assertThat(row["name"]).isEqualTo("zoe")
  }

  @Test
  fun `writeBlob then readRows round-trips an encrypted blob with single wrapping`(): Unit =
    runBlocking {
      val kekUri = "fake-kms://uniform-kek"
      // Single-wrapping mode: parquet wraps each DEK directly with the master key
      // (no intermediate KEK). The bridge must round-trip identically either way.
      val conf =
        Configuration().apply {
          set("parquet.encryption.uniform.key", kekUri)
          setBoolean("parquet.encryption.double.wrapping", false)
        }
      val client = newEncryptingClient(conf, fakeKms(kekUri))

      client.writeBlob("enc-single.parquet", flowOf(encRow()))

      val row = client.getBlob("enc-single.parquet")!!.readRows().toList().single().toNative()
      assertThat(row["id"]).isEqualTo(7L)
      assertThat(row["name"]).isEqualTo("zoe")
    }

  @Test
  fun `readRows on an encrypted blob without the crypto config fails`(): Unit = runBlocking {
    val kekUri = "fake-kms://uniform-kek"
    val conf = Configuration().apply { set("parquet.encryption.uniform.key", kekUri) }
    newEncryptingClient(conf, fakeKms(kekUri)).writeBlob("enc.parquet", flowOf(encRow()))

    // A plaintext client (no crypto factory on its conf) cannot read the rows.
    assertFailsWith<Throwable> { newClient().getBlob("enc.parquet")!!.readRows().toList() }
  }

  @Test
  fun `readKeyValueMetadata works on a plaintext-footer encrypted blob`(): Unit = runBlocking {
    val kekUri = "fake-kms://uniform-kek"
    val conf =
      Configuration().apply {
        set("parquet.encryption.uniform.key", kekUri)
        setBoolean("parquet.encryption.plaintext.footer", true)
      }
    newEncryptingClient(conf, fakeKms(kekUri)).writeBlob("enc.parquet", flowOf(encRow()))

    // Plaintext footer stays readable without keys.
    assertThat(newClient().getBlob("enc.parquet")!!.readKeyValueMetadata()).isNotNull()
  }

  @Test
  fun `writeBlob keyValueMetadata round-trips through a plaintext-footer encrypted blob`(): Unit =
    runBlocking {
      val kekUri = "fake-kms://uniform-kek"
      // PLAINTEXT_FOOTER keeps the footer (and its key-value metadata) readable
      // without keys, so the writeBlob(keyValueMetadata) overload composes with
      // encryption: an encrypting client embeds the metadata and a fresh
      // plaintext client reads the exact entries back with no crypto config.
      val conf =
        Configuration().apply {
          set("parquet.encryption.uniform.key", kekUri)
          setBoolean("parquet.encryption.plaintext.footer", true)
        }
      newEncryptingClient(conf, fakeKms(kekUri))
        .writeBlob(
          "enc-meta.parquet",
          flowOf(encRow()),
          mapOf("edpa.kek_uri" to "fake-kms://kek", "shard" to "3"),
        )

      assertThat(newClient().getBlob("enc-meta.parquet")!!.readKeyValueMetadata())
        .containsAtLeast("edpa.kek_uri", "fake-kms://kek", "shard", "3")
    }

  @Test
  fun `keyMapping resolves a short master-key name to a Tink URI`(): Unit = runBlocking {
    val kekUri = "fake-kms://uniform-kek"
    // The file references the short name "uniform-key" (parquet's column.keys
    // format splits on ':', so a full URI can't be used there); the bridge maps
    // it to the full Tink URI the FakeKmsClient actually supports.
    val conf = Configuration().apply { set("parquet.encryption.uniform.key", "uniform-key") }
    val client =
      newEncryptingClient(conf, fakeKms(kekUri), keyMapping = mapOf("uniform-key" to kekUri))

    client.writeBlob("enc.parquet", flowOf(encRow()))

    assertThat(client.getBlob("enc.parquet")!!.readRows().toList().single().toNative()["name"])
      .isEqualTo("zoe")
  }

  @Test
  fun `per-column key encrypts the configured column`(): Unit = runBlocking {
    val footerKek = "fake-kms://footer-kek"
    val colKek = "fake-kms://col-kek"
    // column.keys uses short master-key names (it splits on ':' / ';'); the
    // bridge resolves them to URIs via keyMapping.
    val keyMapping = mapOf("footer-key" to footerKek, "col-key" to colKek)
    val conf =
      Configuration().apply {
        set("parquet.encryption.footer.key", "footer-key")
        set("parquet.encryption.column.keys", "col-key:name")
        setBoolean("parquet.encryption.plaintext.footer", true)
      }
    val client = newEncryptingClient(conf, fakeKms(footerKek, colKek), keyMapping)

    client.writeBlob("enc.parquet", flowOf(encRow()))

    // Per-column key config is accepted and the blob round-trips through a client
    // holding both the footer and column keys (resolved from short names via
    // keyMapping). A plaintext client cannot read the encrypted blob at all.
    assertThat(client.getBlob("enc.parquet")!!.readRows().toList().single().toNative()["name"])
      .isEqualTo("zoe")
    assertFailsWith<Throwable> { newClient().getBlob("enc.parquet")!!.readRows().toList() }
  }

  // ===== Schema-shape rejections =====

  @Test
  fun `repeated field throws at row time`(): Unit = runBlocking {
    val schema =
      MessageTypeParser.parseMessageType(
        """message Row { required int64 id; repeated int64 tags; }"""
      )
    val row =
      groupOf(schema) {
        add("id", 1L)
        add("tags", 10L)
        add("tags", 20L)
      }
    val key = writeParquetBlob("rep.parquet", schema, listOf(row), null, emptyMap(), null)

    assertFailsWith<IllegalStateException> { newClient().getBlob(key)!!.readRows().toList() }
  }

  @Test
  fun `nested message field throws at row time`(): Unit = runBlocking {
    val schema =
      MessageTypeParser.parseMessageType(
        """
        message Row {
          required int64 id;
          required group nested { required int64 inner_id; }
        }
        """
          .trimIndent()
      )
    val row =
      groupOf(schema) {
        add("id", 1L)
        addGroup("nested").add("inner_id", 99L)
      }
    val key = writeParquetBlob("nest.parquet", schema, listOf(row), null, emptyMap(), null)

    assertFailsWith<IllegalStateException> { newClient().getBlob(key)!!.readRows().toList() }
  }

  // ===== ENCRYPTED_FOOTER rejection =====

  @Test
  fun `readKeyValueMetadata rejects ENCRYPTED_FOOTER mode with a clear error`(): Unit =
    runBlocking {
      val footerKey = randomAesKey(32)
      val encryption =
        FileEncryptionProperties.builder(footerKey).build() // default ENCRYPTED_FOOTER
      val schema = MessageTypeParser.parseMessageType("""message Row { required int64 id; }""")
      val row = groupOf(schema) { add("id", 1L) }
      val key =
        writeParquetBlob(
          "pme-enc-footer.parquet",
          schema,
          listOf(row),
          encryption,
          emptyMap(),
          null,
        )

      assertFailsWith<IllegalStateException> { newClient().getBlob(key)!!.readKeyValueMetadata() }
    }

  // ===== Helpers =====

  private val BYTES_AB = ByteString.copyFrom(byteArrayOf(0xAB.toByte()))
  private val BYTES_CD = ByteString.copyFrom(byteArrayOf(0xCD.toByte()))
  private val BYTES_EF = ByteString.copyFrom(byteArrayOf(0xEF.toByte()))

  private val SAMPLE_SCHEMA: MessageType =
    MessageTypeParser.parseMessageType(
      """
      message Row {
        required int64 id;
        required binary name (STRING);
        required binary data;
      }
      """
        .trimIndent()
    )

  private fun sampleRows(schema: MessageType): List<Group> =
    listOf(
      groupOf(schema) {
        add("id", 1L)
        add("name", "alice")
        add("data", Binary.fromConstantByteArray(byteArrayOf(0xAB.toByte())))
      },
      groupOf(schema) {
        add("id", 2L)
        add("name", "bob")
        add("data", Binary.fromConstantByteArray(byteArrayOf(0xCD.toByte())))
      },
      groupOf(schema) {
        add("id", 3L)
        add("name", "carol")
        add("data", Binary.fromConstantByteArray(byteArrayOf(0xEF.toByte())))
      },
    )

  private fun plaintextSample(name: String, metadata: Map<String, String> = emptyMap()): String =
    writeParquetBlob(name, SAMPLE_SCHEMA, sampleRows(SAMPLE_SCHEMA), null, metadata, null)

  /** Writes a parquet file directly into [tempDir] and returns its blob key. */
  private fun writeParquetBlob(
    name: String,
    schema: MessageType,
    rows: List<Group>,
    encryption: FileEncryptionProperties?,
    metadata: Map<String, String>,
    rowGroupSize: Long?,
  ): String {
    val file = File(tempDir.root, name)
    val builder =
      ExampleParquetWriter.builder(LocalOutputFile(file.toPath()))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withExtraMetaData(metadata)
    if (encryption != null) builder.withEncryption(encryption)
    if (rowGroupSize != null) builder.withRowGroupSize(rowGroupSize)
    builder.build().use { writer -> rows.forEach { writer.write(it) } }
    return name
  }

  private fun groupOf(schema: MessageType, block: Group.() -> Unit): Group {
    val g = SimpleGroupFactory(schema).newGroup()
    g.block()
    return g
  }

  private fun randomAesKey(bytes: Int): ByteArray {
    val k = ByteArray(bytes)
    SecureRandom().nextBytes(k)
    return k
  }

  companion object {
    init {
      // Static one-time Tink Aead registration shared by every test.
      AeadConfig.register()
    }
  }
}

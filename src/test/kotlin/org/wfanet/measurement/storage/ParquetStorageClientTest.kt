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
import com.google.protobuf.Timestamp
import com.google.type.Date
import java.io.File
import java.security.SecureRandom
import java.time.Instant
import java.time.LocalDate
import java.util.Base64
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.crypto.ParquetCipher
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

@RunWith(JUnit4::class)
class ParquetStorageClientTest {
  @Rule @JvmField val tempDir = TemporaryFolder()

  private fun newClient(
    decryptionConfig: ParquetDecryptionConfig? = null,
    encryption: (suspend (String) -> FileEncryptionProperties?)? = null,
  ): ParquetStorageClient =
    ParquetStorageClient(
      Configuration(),
      Path(tempDir.root.absolutePath),
      decryptionConfig = decryptionConfig,
      encryptionPropertiesProvider = encryption,
    )

  /**
   * Builds a [ParquetDecryptionConfig] (via a [FakeKmsClient]) plus the footer
   * metadata that wraps [footerKey] under it — write the metadata into a PME
   * blob and the config can bootstrap the footer key from it.
   */
  private fun kmsConfigFor(footerKey: ByteArray): Pair<ParquetDecryptionConfig, Map<String, String>> {
    AeadConfig.register()
    val kekUri = "fake-kms://kek1"
    val kekAead: Aead =
      KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM")).getPrimitive(Aead::class.java)
    val kmsClient = FakeKmsClient().also { it.setAead(kekUri, kekAead) }
    val metadata =
      mapOf(
        "edpa.kek_uri" to kekUri,
        "edpa.encrypted_dek" to
          Base64.getEncoder().encodeToString(kekAead.encrypt(footerKey, ByteArray(0))),
      )
    return ParquetDecryptionConfig(kmsClient) to metadata
  }

  // ===== Plaintext reads =====

  @Test
  fun `readRows on plaintext blob returns native-typed values keyed by column name`(): Unit =
    runBlocking {
      val key = plaintextSample("data.parquet")

      val rows = newClient().getBlob(key)!!.readRows().toList()

      assertThat(rows).hasSize(3)
      assertThat(rows[0]).containsExactly("id", 1L, "name", "alice", "data", BYTES_AB)
      assertThat(rows[1]).containsExactly("id", 2L, "name", "bob", "data", BYTES_CD)
      assertThat(rows[2]).containsExactly("id", 3L, "name", "carol", "data", BYTES_EF)
    }

  @Test
  fun `readKeyValueMetadata returns plaintext footer entries`(): Unit = runBlocking {
    val key = plaintextSample("data.parquet", metadata = mapOf("foo" to "bar"))

    assertThat(newClient().getBlob(key)!!.readKeyValueMetadata()).containsAtLeast("foo", "bar")
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

    assertThat(keys)
      .containsExactly("done", "file1.parquet", "file2.parquet", "sub/file3.parquet")
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
    val timestamp =
      // Micro-aligned nanos: the canonical write precision is MICROS, so a
      // non-micro nanos value would not survive the round trip.
      Timestamp.newBuilder().setSeconds(1_700_000_000L).setNanos(123_456_000).build()
    val date = Date.newBuilder().setYear(2026).setMonth(6).setDay(10).build()
    val row =
      ParquetRow.newBuilder()
        .putColumns("i32", ParquetValue.newBuilder().setInt32Value(7).build())
        .putColumns("i64", ParquetValue.newBuilder().setInt64Value(8L).build())
        .putColumns("f32", ParquetValue.newBuilder().setFloatValue(1.5f).build())
        .putColumns("f64", ParquetValue.newBuilder().setDoubleValue(2.5).build())
        .putColumns("flag", ParquetValue.newBuilder().setBoolValue(true).build())
        .putColumns("s", ParquetValue.newBuilder().setStringValue("hi").build())
        .putColumns(
          "b",
          ParquetValue.newBuilder()
            .setBytesValue(ByteString.copyFrom(byteArrayOf(0x01, 0x02, 0x03)))
            .build(),
        )
        .putColumns("t", ParquetValue.newBuilder().setTimestampValue(timestamp).build())
        .putColumns("d", ParquetValue.newBuilder().setDateValue(date).build())
        .build()

    val client = newClient()
    client.writeBlob("rt.parquet", flowOf(row.toByteString()))
    val blob = client.getBlob("rt.parquet")!!

    // read() round-trips the ParquetRow proto.
    val readBack = blob.read().toList().map { ParquetRow.parseFrom(it) }
    assertThat(readBack).containsExactly(row)

    // readRows() projects the same row into native types.
    val native = blob.readRows().toList().single()
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
    val row1 =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(1L).build())
        .putColumns("name", ParquetValue.newBuilder().setStringValue("alice").build())
        .build()
    val row2 =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(2L).build())
        .putColumns("name", ParquetValue.getDefaultInstance()) // explicit NULL
        .build()

    val client = newClient()
    client.writeBlob("rows.parquet", flowOf(row1.toByteString(), row2.toByteString()))

    val readBack = client.getBlob("rows.parquet")!!.read().toList().map { ParquetRow.parseFrom(it) }
    assertThat(readBack).containsExactly(row1, row2).inOrder()
  }

  @Test
  fun `writeBlob rejects a later row with a column absent from the first row`(): Unit = runBlocking {
    val row1 =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(1L).build())
        .build()
    val row2 =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(2L).build())
        .putColumns("extra", ParquetValue.newBuilder().setStringValue("x").build())
        .build()

    val ex =
      assertFailsWith<IllegalArgumentException> {
        newClient().writeBlob("rows.parquet", flowOf(row1.toByteString(), row2.toByteString()))
      }
    assertThat(ex.message).contains("extra")
  }

  @Test
  fun `writeBlob rejects a later row whose column kind differs from the first`(): Unit =
    runBlocking {
      val row1 =
        ParquetRow.newBuilder()
          .putColumns("v", ParquetValue.newBuilder().setInt64Value(1L).build())
          .build()
      val row2 =
        ParquetRow.newBuilder()
          .putColumns("v", ParquetValue.newBuilder().setStringValue("two").build())
          .build()

      val ex =
        assertFailsWith<IllegalArgumentException> {
          newClient().writeBlob("rows.parquet", flowOf(row1.toByteString(), row2.toByteString()))
        }
      assertThat(ex.message).contains("v")
    }

  @Test
  fun `writeBlob failure deletes the partial blob`(): Unit = runBlocking {
    val client = newClient()
    // Row 1 is valid (creates the output file); row 2 fails validation, which
    // must trigger cleanup so no half-written blob is left behind.
    val row1 =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(1L).build())
        .build()
    val row2 =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(2L).build())
        .putColumns("extra", ParquetValue.newBuilder().setStringValue("x").build())
        .build()

    assertFailsWith<IllegalArgumentException> {
      client.writeBlob("partial.parquet", flowOf(row1.toByteString(), row2.toByteString()))
    }

    assertThat(client.getBlob("partial.parquet")).isNull()
  }

  // ===== Primitive-type coverage (reads) =====

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

    val map = newClient().getBlob(key)!!.readRows().toList().single()
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

    assertThat(newClient().getBlob(key)!!.readRows().toList().single()["j"])
      .isEqualTo("""{"k":1}""")
  }

  @Test
  fun `BSON-annotated BINARY decodes to ByteString`(): Unit = runBlocking {
    val schema = MessageTypeParser.parseMessageType("""message Row { required binary b (BSON); }""")
    val raw = byteArrayOf(0x11, 0x22, 0x33)
    val row = groupOf(schema) { add("b", Binary.fromConstantByteArray(raw)) }
    val key = writeParquetBlob("bson.parquet", schema, listOf(row), null, emptyMap(), null)

    // BSON is bytes-encoded (not UTF-8 text) -> kept as ByteString.
    assertThat(newClient().getBlob(key)!!.readRows().toList().single()["b"])
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

    val rows = newClient().getBlob(key)!!.readRows().toList()
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

    val outRows = newClient().getBlob(key)!!.readRows().toList()
    assertThat(outRows).hasSize(200)
    assertThat(outRows.first()).containsExactly("n", 0L, "s", "row-0")
    assertThat(outRows.last()).containsExactly("n", 199L, "s", "row-199")
  }

  // ===== PME paths (declarative ParquetDecryptionConfig) =====

  @Test
  fun `readRows on PME blob decrypts via ParquetDecryptionConfig`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val (config, metadata) = kmsConfigFor(footerKey)
    val key = pmeSample("pme.parquet", footerKey, metadata)

    val rows = newClient(config).getBlob(key)!!.readRows().toList()
    assertThat(rows).hasSize(3)
    assertThat(rows[0]["id"]).isEqualTo(1L)
    assertThat(rows[2]["name"]).isEqualTo("carol")
  }

  @Test
  fun `readRows on PME blob written with AES_GCM_CTR_V1 cipher decrypts correctly`(): Unit =
    runBlocking {
      // PME supports AES_GCM_V1 (default) and AES_GCM_CTR_V1 (CTR for data
      // pages, GCM for metadata). The cipher is recorded in the file and
      // parquet-mr selects it transparently on read.
      val footerKey = randomAesKey(32)
      val (config, metadata) = kmsConfigFor(footerKey)
      val encryption =
        FileEncryptionProperties.builder(footerKey)
          .withPlaintextFooter()
          .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
          .build()
      val key =
        writeParquetBlob(
          "pme-ctr.parquet",
          SAMPLE_SCHEMA,
          sampleRows(SAMPLE_SCHEMA),
          encryption,
          metadata,
          null,
        )

      val rows = newClient(config).getBlob(key)!!.readRows().toList()
      assertThat(rows).hasSize(3)
      assertThat(rows[0]["id"]).isEqualTo(1L)
      assertThat(rows[2]["name"]).isEqualTo("carol")
    }

  @Test
  fun `readKeyValueMetadata on PME blob works without decryption config`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val key = pmeSample("pme.parquet", footerKey, mapOf("dek" to "blob"))

    assertThat(newClient().getBlob(key)!!.readKeyValueMetadata()).containsAtLeast("dek", "blob")
  }

  @Test
  fun `readRows on PME blob without decryption config fails`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val key = pmeSample("pme.parquet", footerKey, emptyMap())

    val ex = assertFailsWith<Throwable> { newClient().getBlob(key)!!.readRows().toList() }
    assertThat(ex.toString()).ignoringCase().contains("encrypt")
  }

  @Test
  fun `ParquetDecryptionConfig with missing metadata key fails clearly`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    // Footer has no edpa.kek_uri / edpa.encrypted_dek entries.
    val key = pmeSample("pme.parquet", footerKey, emptyMap())

    val client = newClient(ParquetDecryptionConfig(FakeKmsClient()))

    val ex = assertFailsWith<IllegalStateException> { client.getBlob(key)!!.readRows().toList() }
    assertThat(ex.message).contains("edpa.kek_uri")
  }

  @Test
  fun `writeBlob with encryption provider produces an encrypted blob`(): Unit = runBlocking {
    // Write-side PME: encrypt columns under a footer key (plaintext footer).
    // The file's footer stays readable, but a plaintext client cannot read the
    // (encrypted) rows — proving encryption was applied on write.
    val footerKey = randomAesKey(32)
    val client =
      newClient(
        encryption = {
          FileEncryptionProperties.builder(footerKey).withPlaintextFooter().build()
        }
      )
    val row =
      ParquetRow.newBuilder()
        .putColumns("id", ParquetValue.newBuilder().setInt64Value(7L).build())
        .putColumns("name", ParquetValue.newBuilder().setStringValue("zoe").build())
        .build()
    client.writeBlob("enc.parquet", flowOf(row.toByteString()))

    // Footer remains readable without keys (plaintext footer)...
    assertThat(newClient().getBlob("enc.parquet")!!.readKeyValueMetadata()).isNotNull()
    // ...but the encrypted rows are unreadable without decryption.
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

    val ex =
      assertFailsWith<IllegalStateException> { newClient().getBlob(key)!!.readRows().toList() }
    assertThat(ex.message).contains("Repeated field 'tags'")
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

    val ex =
      assertFailsWith<IllegalStateException> { newClient().getBlob(key)!!.readRows().toList() }
    assertThat(ex.message).contains("Nested message field 'nested'")
  }

  // ===== ENCRYPTED_FOOTER rejection =====

  @Test
  fun `readKeyValueMetadata rejects ENCRYPTED_FOOTER mode with a clear error`(): Unit =
    runBlocking {
      val footerKey = randomAesKey(32)
      val encryption = FileEncryptionProperties.builder(footerKey).build() // default ENCRYPTED_FOOTER
      val schema = MessageTypeParser.parseMessageType("""message Row { required int64 id; }""")
      val row = groupOf(schema) { add("id", 1L) }
      val key =
        writeParquetBlob("pme-enc-footer.parquet", schema, listOf(row), encryption, emptyMap(), null)

      val ex =
        assertFailsWith<IllegalStateException> {
          newClient().getBlob(key)!!.readKeyValueMetadata()
        }
      assertThat(ex.message).contains("ENCRYPTED_FOOTER")
      assertThat(ex.message).contains("PLAINTEXT_FOOTER")
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

  private fun pmeSample(name: String, footerKey: ByteArray, metadata: Map<String, String>): String {
    val props = FileEncryptionProperties.builder(footerKey).withPlaintextFooter().build()
    return writeParquetBlob(name, SAMPLE_SCHEMA, sampleRows(SAMPLE_SCHEMA), props, metadata, null)
  }

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
}

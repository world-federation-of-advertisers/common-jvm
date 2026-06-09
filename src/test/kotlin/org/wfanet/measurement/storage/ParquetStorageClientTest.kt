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
import java.nio.file.Files
import java.nio.file.Path
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.parquet.crypto.FileDecryptionProperties
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class ParquetStorageClientTest {

  // ===== Plaintext round trip =====

  @Test
  fun `readRows on plaintext blob returns native-typed values keyed by column name`(): Unit =
    runBlocking {
      val blobs = InMemoryStorageClient()
      blobs.writeBlob("data.parquet", flowOf(ByteString.copyFrom(plaintextSampleBytes())))

      ParquetStorageClient(blobs).getBlob("data.parquet")!!.use { blob ->
        val rows = blob.readRows().toList()
        assertThat(rows).hasSize(3)
        assertThat(rows[0]).containsExactly("id", 1L, "name", "alice", "data", BYTES_AB)
        assertThat(rows[1]).containsExactly("id", 2L, "name", "bob", "data", BYTES_CD)
        assertThat(rows[2]).containsExactly("id", 3L, "name", "carol", "data", BYTES_EF)
      }
    }

  @Test
  fun `readKeyValueMetadata returns plaintext footer entries`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "data.parquet",
      flowOf(ByteString.copyFrom(plaintextSampleBytes(metadata = mapOf("foo" to "bar")))),
    )

    ParquetStorageClient(blobs).getBlob("data.parquet")!!.use { blob ->
      assertThat(blob.readKeyValueMetadata()).containsAtLeast("foo", "bar")
    }
  }

  @Test
  fun `getBlob returns null for missing key`(): Unit = runBlocking {
    val client = ParquetStorageClient(InMemoryStorageClient())
    assertThat(client.getBlob("nothing-here.parquet")).isNull()
  }

  @Test
  fun `empty parquet file emits no rows`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "empty.parquet",
      flowOf(
        ByteString.copyFrom(writeParquet(SAMPLE_SCHEMA, emptyList(), null, emptyMap(), null))
      ),
    )

    ParquetStorageClient(blobs).getBlob("empty.parquet")!!.use { blob ->
      assertThat(blob.readRows().toList()).isEmpty()
    }
  }

  // ===== Primitive-type coverage =====

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
      }
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "p.parquet",
      flowOf(ByteString.copyFrom(writeParquet(schema, listOf(row), null, emptyMap(), null))),
    )

    ParquetStorageClient(blobs).getBlob("p.parquet")!!.use { blob ->
      val map = blob.readRows().toList().single()
      assertThat(map["i32"]).isEqualTo(42)
      assertThat(map["i64"]).isEqualTo(1234567890123L)
      assertThat(map["f32"]).isEqualTo(1.5f)
      assertThat(map["f64"]).isEqualTo(2.5)
      assertThat(map["flag"]).isEqualTo(true)
      assertThat(map["str"]).isEqualTo("hello")
      // ENUM-annotated BINARY must decode to String (typed `is`-check
      // covers EnumLogicalTypeAnnotation, not just startsWith("STRING")).
      assertThat(map["enm"]).isEqualTo("RED")
      assertThat(map["raw"]).isEqualTo(ByteString.copyFrom(byteArrayOf(0x01, 0x02, 0x03)))
      assertThat(map["fixed4"])
        .isEqualTo(ByteString.copyFrom(byteArrayOf(0x0A, 0x0B, 0x0C, 0x0D)))
    }
  }

  @Test
  fun `JSON-annotated BINARY decodes to String`(): Unit = runBlocking {
    val schema = MessageTypeParser.parseMessageType("""message Row { required binary j (JSON); }""")
    val row = groupOf(schema) { add("j", """{"k":1}""") }
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "json.parquet",
      flowOf(ByteString.copyFrom(writeParquet(schema, listOf(row), null, emptyMap(), null))),
    )

    ParquetStorageClient(blobs).getBlob("json.parquet")!!.use { blob ->
      assertThat(blob.readRows().toList().single()["j"]).isEqualTo("""{"k":1}""")
    }
  }

  @Test
  fun `BSON-annotated BINARY decodes to ByteString`(): Unit = runBlocking {
    val schema = MessageTypeParser.parseMessageType("""message Row { required binary b (BSON); }""")
    val raw = byteArrayOf(0x11, 0x22, 0x33)
    val row = groupOf(schema) { add("b", Binary.fromConstantByteArray(raw)) }
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "bson.parquet",
      flowOf(ByteString.copyFrom(writeParquet(schema, listOf(row), null, emptyMap(), null))),
    )

    ParquetStorageClient(blobs).getBlob("bson.parquet")!!.use { blob ->
      // BSON is bytes-encoded (not UTF-8 text) -> kept as ByteString.
      assertThat(blob.readRows().toList().single()["b"]).isEqualTo(ByteString.copyFrom(raw))
    }
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
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "opt.parquet",
      flowOf(
        ByteString.copyFrom(
          writeParquet(schema, listOf(rowWithoutMaybe, rowWithMaybe), null, emptyMap(), null)
        )
      ),
    )

    ParquetStorageClient(blobs).getBlob("opt.parquet")!!.use { blob ->
      val rows = blob.readRows().toList()
      assertThat(rows[0]).containsExactly("id", 1L, "maybe", null)
      assertThat(rows[1]).containsExactly("id", 2L, "maybe", "present")
    }
  }

  @Test
  fun `multi-row-group file reads all rows in order`(): Unit = runBlocking {
    // Force multiple row groups by setting a tiny row-group size so each
    // few rows trigger a new group. Reading across row groups exercises
    // parquet's seek-back path against the temp file.
    val schema =
      MessageTypeParser.parseMessageType("""message Row { required int64 n; required binary s (STRING); }""")
    val rows =
      (0 until 200).map { i ->
        groupOf(schema) {
          add("n", i.toLong())
          add("s", "row-$i")
        }
      }
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "rg.parquet",
      flowOf(
        ByteString.copyFrom(writeParquet(schema, rows, null, emptyMap(), rowGroupSize = 1024L))
      ),
    )

    ParquetStorageClient(blobs).getBlob("rg.parquet")!!.use { blob ->
      val outRows = blob.readRows().toList()
      assertThat(outRows).hasSize(200)
      assertThat(outRows.first()).containsExactly("n", 0L, "s", "row-0")
      assertThat(outRows.last()).containsExactly("n", 199L, "s", "row-199")
    }
  }

  // ===== PME paths =====

  @Test
  fun `readRows on PME blob with correct footer key returns rows`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("pme.parquet", flowOf(ByteString.copyFrom(pmeSampleBytes(footerKey, emptyMap()))))

    val client =
      ParquetStorageClient(blobs) {
        FileDecryptionProperties.builder().withFooterKey(footerKey).build()
      }

    client.getBlob("pme.parquet")!!.use { blob ->
      val rows = blob.readRows().toList()
      assertThat(rows).hasSize(3)
      assertThat(rows[0]["id"]).isEqualTo(1L)
      assertThat(rows[2]["name"]).isEqualTo("carol")
    }
  }

  @Test
  fun `readKeyValueMetadata on PME blob works without decryption provider`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "pme.parquet",
      flowOf(ByteString.copyFrom(pmeSampleBytes(footerKey, mapOf("dek" to "blob")))),
    )

    ParquetStorageClient(blobs).getBlob("pme.parquet")!!.use { blob ->
      assertThat(blob.readKeyValueMetadata()).containsAtLeast("dek", "blob")
    }
  }

  @Test
  fun `readRows on PME blob without decryption provider fails`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("pme.parquet", flowOf(ByteString.copyFrom(pmeSampleBytes(footerKey, emptyMap()))))

    val ex =
      assertFailsWith<Throwable> {
        ParquetStorageClient(blobs).getBlob("pme.parquet")!!.use { blob -> blob.readRows().toList() }
      }
    assertThat(ex.toString()).ignoringCase().contains("encrypt")
  }

  @Test
  fun `decryption provider returning null on PME blob fails readRows`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("pme.parquet", flowOf(ByteString.copyFrom(pmeSampleBytes(footerKey, emptyMap()))))

    // null from provider means "treat as plaintext" — for an encrypted
    // blob, parquet-mr surfaces a crypto error at read time.
    val client = ParquetStorageClient(blobs) { null }
    val ex =
      assertFailsWith<Throwable> {
        client.getBlob("pme.parquet")!!.use { blob -> blob.readRows().toList() }
      }
    assertThat(ex.toString()).ignoringCase().contains("encrypt")
  }

  @Test
  fun `decryption provider exception is propagated`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("pme.parquet", flowOf(ByteString.copyFrom(pmeSampleBytes(footerKey, emptyMap()))))

    val client = ParquetStorageClient(blobs) { error("kms unreachable") }
    val ex =
      assertFailsWith<IllegalStateException> {
        client.getBlob("pme.parquet")!!.use { blob -> blob.readRows().toList() }
      }
    assertThat(ex.message).contains("kms unreachable")
  }

  @Test
  fun `KMS-driven decryption provider round-trip`(): Unit = runBlocking {
    AeadConfig.register()
    val kekUri = "fake-kms://kek1"
    val kekHandle = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM"))
    val kekAead: Aead = kekHandle.getPrimitive(Aead::class.java)
    val kmsClient = FakeKmsClient().also { it.setAead(kekUri, kekAead) }

    val footerKey = randomAesKey(32)
    val encryptedDek: ByteArray = kekAead.encrypt(footerKey, ByteArray(0))
    val metadata =
      mapOf(
        "edpa.kek_uri" to kekUri,
        "edpa.encrypted_dek" to Base64.getEncoder().encodeToString(encryptedDek),
      )

    val blobs = InMemoryStorageClient()
    blobs.writeBlob("pme.parquet", flowOf(ByteString.copyFrom(pmeSampleBytes(footerKey, metadata))))

    val client =
      ParquetStorageClient(blobs) { blob ->
        // Provider re-entry pattern: allowed to call readKeyValueMetadata
        // on the same blob (different mutex). MUST NOT call readRows.
        val md = blob.readKeyValueMetadata()
        val kek = md.getValue("edpa.kek_uri")
        val ciphertext = Base64.getDecoder().decode(md.getValue("edpa.encrypted_dek"))
        val raw = kmsClient.getAead(kek).decrypt(ciphertext, ByteArray(0))
        FileDecryptionProperties.builder().withFooterKey(raw).build()
      }

    client.getBlob("pme.parquet")!!.use { blob ->
      val rows = blob.readRows().toList()
      assertThat(rows).hasSize(3)
      assertThat(rows[1]["name"]).isEqualTo("bob")
      // Caller can re-read the footer; same temp file is reused.
      assertThat(blob.readKeyValueMetadata()).containsAtLeast("edpa.kek_uri", kekUri)
    }
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
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "rep.parquet",
      flowOf(ByteString.copyFrom(writeParquet(schema, listOf(row), null, emptyMap(), null))),
    )

    val ex =
      assertFailsWith<IllegalStateException> {
        ParquetStorageClient(blobs).getBlob("rep.parquet")!!.use { it.readRows().toList() }
      }
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
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "nest.parquet",
      flowOf(ByteString.copyFrom(writeParquet(schema, listOf(row), null, emptyMap(), null))),
    )

    val ex =
      assertFailsWith<IllegalStateException> {
        ParquetStorageClient(blobs).getBlob("nest.parquet")!!.use { it.readRows().toList() }
      }
    assertThat(ex.message).contains("Nested message field 'nested'")
  }

  // ===== Lifecycle =====

  @Test
  fun `multiple reads on the same blob share one underlying download`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "data.parquet",
      flowOf(ByteString.copyFrom(plaintextSampleBytes(metadata = mapOf("foo" to "bar")))),
    )
    val counting = ReadCountingStorageClient(blobs)

    ParquetStorageClient(counting).getBlob("data.parquet")!!.use { blob ->
      blob.readRows().toList()
      blob.readKeyValueMetadata()
      blob.readRows().toList()
    }
    assertThat(counting.readCount.get()).isEqualTo(1)
  }

  @Test
  fun `two getBlob calls for the same key download twice`(): Unit = runBlocking {
    // Locks in the documented "fresh ParquetBlob per getBlob call"
    // behavior — getBlob does NOT share temp files across calls.
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("data.parquet", flowOf(ByteString.copyFrom(plaintextSampleBytes())))
    val counting = ReadCountingStorageClient(blobs)
    val client = ParquetStorageClient(counting)

    client.getBlob("data.parquet")!!.use { it.readRows().toList() }
    client.getBlob("data.parquet")!!.use { it.readRows().toList() }
    assertThat(counting.readCount.get()).isEqualTo(2)
  }

  @Test
  fun `two ParquetBlobs from one client are independent`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "a.parquet",
      flowOf(ByteString.copyFrom(plaintextSampleBytes(metadata = mapOf("which" to "a")))),
    )
    blobs.writeBlob(
      "b.parquet",
      flowOf(ByteString.copyFrom(plaintextSampleBytes(metadata = mapOf("which" to "b")))),
    )
    val client = ParquetStorageClient(blobs)

    val a = client.getBlob("a.parquet")!!
    val b = client.getBlob("b.parquet")!!
    try {
      assertThat(a.readKeyValueMetadata()).containsAtLeast("which", "a")
      assertThat(b.readKeyValueMetadata()).containsAtLeast("which", "b")
      // Closing one must not affect the other.
      a.close()
      assertFailsWith<IllegalStateException> { a.readRows().toList() }
      assertThat(b.readRows().toList()).hasSize(3)
    } finally {
      a.close()
      b.close()
    }
  }

  @Test
  fun `readRows after close throws`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("data.parquet", flowOf(ByteString.copyFrom(plaintextSampleBytes())))

    val blob = ParquetStorageClient(blobs).getBlob("data.parquet")!!
    blob.readRows().toList()
    blob.close()
    val ex = assertFailsWith<IllegalStateException> { blob.readRows().toList() }
    assertThat(ex.message).contains("closed")
  }

  @Test
  fun `readKeyValueMetadata after close throws`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("data.parquet", flowOf(ByteString.copyFrom(plaintextSampleBytes())))

    val blob = ParquetStorageClient(blobs).getBlob("data.parquet")!!
    blob.readKeyValueMetadata()
    blob.close()
    val ex = assertFailsWith<IllegalStateException> { blob.readKeyValueMetadata() }
    assertThat(ex.message).contains("closed")
  }

  @Test
  fun `double close is idempotent`(): Unit = runBlocking {
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("data.parquet", flowOf(ByteString.copyFrom(plaintextSampleBytes())))

    val blob = ParquetStorageClient(blobs).getBlob("data.parquet")!!
    blob.readRows().toList()
    blob.close()
    blob.close() // must not throw
  }

  // ===== ENCRYPTED_FOOTER rejection =====

  @Test
  fun `readKeyValueMetadata rejects ENCRYPTED_FOOTER mode with a clear error`(): Unit =
    runBlocking {
      val footerKey = randomAesKey(32)
      val encryption =
        FileEncryptionProperties.builder(footerKey).build() // default = ENCRYPTED_FOOTER
      val schema = MessageTypeParser.parseMessageType("""message Row { required int64 id; }""")
      val row = groupOf(schema) { add("id", 1L) }
      val blobs = InMemoryStorageClient()
      blobs.writeBlob(
        "pme-enc-footer.parquet",
        flowOf(ByteString.copyFrom(writeParquet(schema, listOf(row), encryption, emptyMap(), null))),
      )

      val ex =
        assertFailsWith<IllegalStateException> {
          ParquetStorageClient(blobs).getBlob("pme-enc-footer.parquet")!!.use { blob ->
            blob.readKeyValueMetadata()
          }
        }
      assertThat(ex.message).contains("ENCRYPTED_FOOTER")
      assertThat(ex.message).contains("PLAINTEXT_FOOTER")
    }

  // ===== Helpers =====

  /**
   * Test-only [StorageClient] wrapper that counts how many times any of
   * its blobs' `read()` flows are subscribed to. Used to verify
   * download-frequency behavior (cache hit per ParquetBlob, no cache
   * across getBlob calls).
   */
  private class ReadCountingStorageClient(private val delegate: StorageClient) : StorageClient {
    val readCount: AtomicInteger = AtomicInteger(0)

    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>) =
      delegate.writeBlob(blobKey, content)

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
      val raw = delegate.getBlob(blobKey) ?: return null
      return CountingBlob(raw)
    }

    override suspend fun listBlobs(prefix: String?) = delegate.listBlobs(prefix)

    private inner class CountingBlob(private val raw: StorageClient.Blob) : StorageClient.Blob by raw {
      override fun read(): Flow<ByteString> {
        readCount.incrementAndGet()
        return raw.read()
      }
    }
  }

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

  private fun plaintextSampleBytes(metadata: Map<String, String> = emptyMap()): ByteArray =
    writeParquet(SAMPLE_SCHEMA, sampleRows(SAMPLE_SCHEMA), null, metadata, null)

  private fun pmeSampleBytes(footerKey: ByteArray, metadata: Map<String, String>): ByteArray {
    val props = FileEncryptionProperties.builder(footerKey).withPlaintextFooter().build()
    return writeParquet(SAMPLE_SCHEMA, sampleRows(SAMPLE_SCHEMA), props, metadata, null)
  }

  private fun writeParquet(
    schema: MessageType,
    rows: List<Group>,
    encryption: FileEncryptionProperties?,
    metadata: Map<String, String>,
    rowGroupSize: Long?,
  ): ByteArray {
    val path: Path = Files.createTempFile("pst-writer-", ".parquet")
    // LocalOutputFile refuses to overwrite, so delete the empty file first.
    Files.deleteIfExists(path)
    try {
      val builder =
        ExampleParquetWriter.builder(LocalOutputFile(path))
          .withType(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withExtraMetaData(metadata)
      if (encryption != null) builder.withEncryption(encryption)
      if (rowGroupSize != null) builder.withRowGroupSize(rowGroupSize)
      builder.build().use { writer -> rows.forEach { writer.write(it) } }
      return Files.readAllBytes(path)
    } finally {
      Files.deleteIfExists(path)
    }
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

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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.parquet.crypto.FileDecryptionProperties
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class ParquetStorageClientTest {

  // ===== Plaintext path =====

  @Test
  fun `readRows on plaintext blob returns rows as Map of column to native value`(): Unit =
    runBlocking {
      val blobs = InMemoryStorageClient()
      val key = "data.parquet"
      blobs.writeBlob(key, flowOf(ByteString.copyFrom(plaintextParquetBytes())))

      ParquetStorageClient(blobs).getBlob(key)!!.use { blob ->
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
    val key = "data.parquet"
    blobs.writeBlob(
      key,
      flowOf(ByteString.copyFrom(plaintextParquetBytes(metadata = mapOf("foo" to "bar")))),
    )

    ParquetStorageClient(blobs).getBlob(key)!!.use { blob ->
      assertThat(blob.readKeyValueMetadata()).containsAtLeast("foo", "bar")
    }
  }

  // ===== PME path =====

  @Test
  fun `readRows on PME blob with correct footer key returns rows`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    val key = "pme.parquet"
    blobs.writeBlob(key, flowOf(ByteString.copyFrom(pmeParquetBytes(footerKey, emptyMap()))))

    val client =
      ParquetStorageClient(blobs) { _ ->
        FileDecryptionProperties.builder().withFooterKey(footerKey).build()
      }

    client.getBlob(key)!!.use { blob ->
      val rows = blob.readRows().toList()
      assertThat(rows).hasSize(3)
      assertThat(rows[0]["id"]).isEqualTo(1L)
      assertThat(rows[2]["name"]).isEqualTo("carol")
    }
  }

  @Test
  fun `readKeyValueMetadata on PME blob works without decryption provider`(): Unit = runBlocking {
    // Footer is plaintext under PLAINTEXT_FOOTER mode — we should be able
    // to read key-value entries WITHOUT setting up decryption. This is the
    // bootstrap point that lets a caller read an encrypted DEK out of the
    // footer before configuring column decryption.
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    val key = "pme.parquet"
    blobs.writeBlob(
      key,
      flowOf(ByteString.copyFrom(pmeParquetBytes(footerKey, mapOf("dek" to "blob")))),
    )

    ParquetStorageClient(blobs).getBlob(key)!!.use { blob ->
      assertThat(blob.readKeyValueMetadata()).containsAtLeast("dek", "blob")
    }
  }

  @Test
  fun `readRows on PME blob without decryption provider fails`(): Unit = runBlocking {
    val footerKey = randomAesKey(32)
    val blobs = InMemoryStorageClient()
    val key = "pme.parquet"
    blobs.writeBlob(key, flowOf(ByteString.copyFrom(pmeParquetBytes(footerKey, emptyMap()))))

    val client = ParquetStorageClient(blobs) // null provider
    val exception =
      assertFailsWith<Throwable> {
        client.getBlob(key)!!.use { blob -> blob.readRows().toList() }
      }
    // parquet-mr surfaces this as ParquetCryptoRuntimeException or similar.
    assertThat(exception.toString()).ignoringCase().contains("encrypt")
  }

  @Test
  fun `KMS-driven decryption provider round-trip`(): Unit = runBlocking {
    AeadConfig.register()
    val kekUri = "fake-kms://kek1"
    val kekHandle =
      KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM"))
    val kekAead: Aead = kekHandle.getPrimitive(Aead::class.java)
    val kmsClient = FakeKmsClient().also { it.setAead(kekUri, kekAead) }

    val footerKey = randomAesKey(32)
    // The caller's writer wraps the raw AES key directly with KMS Aead and
    // stashes it (base64) in the plaintext footer. NO Tink keyset format
    // around it — just raw AES bytes encrypted by KMS.
    val encryptedDek: ByteArray = kekAead.encrypt(footerKey, ByteArray(0))
    val metadata =
      mapOf(
        "edpa.kek_uri" to kekUri,
        "edpa.encrypted_dek" to Base64.getEncoder().encodeToString(encryptedDek),
      )

    val blobs = InMemoryStorageClient()
    val key = "pme.parquet"
    blobs.writeBlob(key, flowOf(ByteString.copyFrom(pmeParquetBytes(footerKey, metadata))))

    val client =
      ParquetStorageClient(blobs) { blob ->
        val md = blob.readKeyValueMetadata()
        val kek = md.getValue("edpa.kek_uri")
        val ciphertext = Base64.getDecoder().decode(md.getValue("edpa.encrypted_dek"))
        val raw = kmsClient.getAead(kek).decrypt(ciphertext, ByteArray(0))
        FileDecryptionProperties.builder().withFooterKey(raw).build()
      }

    client.getBlob(key)!!.use { blob ->
      val rows = blob.readRows().toList()
      assertThat(rows).hasSize(3)
      assertThat(rows[1]["name"]).isEqualTo("bob")
      // Caller can still read the footer back too, sharing the same temp file.
      assertThat(blob.readKeyValueMetadata()).containsAtLeast("edpa.kek_uri", kekUri)
    }
  }

  // ===== Reject nested / repeated =====

  @Test
  fun `repeated field throws at row time`(): Unit = runBlocking {
    val schema: MessageType =
      MessageTypeParser.parseMessageType(
        """message Row { required int64 id; repeated int64 tags; }"""
      )
    val rows =
      listOf(
        groupOf(schema) {
          add("id", 1L)
          add("tags", 10L)
          add("tags", 20L)
        }
      )
    val blobs = InMemoryStorageClient()
    val key = "rep.parquet"
    blobs.writeBlob(key, flowOf(ByteString.copyFrom(writeParquet(schema, rows, null, emptyMap()))))

    val ex =
      assertFailsWith<IllegalStateException> {
        ParquetStorageClient(blobs).getBlob(key)!!.use { it.readRows().toList() }
      }
    assertThat(ex.message).contains("Repeated field 'tags'")
  }

  @Test
  fun `nested message field throws at row time`(): Unit = runBlocking {
    val schema: MessageType =
      MessageTypeParser.parseMessageType(
        """
        message Row {
          required int64 id;
          required group nested { required int64 inner_id; }
        }
        """
          .trimIndent()
      )
    val rows =
      listOf(
        groupOf(schema) {
          add("id", 1L)
          addGroup("nested").add("inner_id", 99L)
        }
      )
    val blobs = InMemoryStorageClient()
    val key = "nest.parquet"
    blobs.writeBlob(key, flowOf(ByteString.copyFrom(writeParquet(schema, rows, null, emptyMap()))))

    val ex =
      assertFailsWith<IllegalStateException> {
        ParquetStorageClient(blobs).getBlob(key)!!.use { it.readRows().toList() }
      }
    assertThat(ex.message).contains("Nested message field 'nested'")
  }

  // ===== Temp-file lifecycle =====

  @Test
  fun `close deletes the temp file`(): Unit = runBlocking {
    val before = countTempFiles()
    val blobs = InMemoryStorageClient()
    blobs.writeBlob("data.parquet", flowOf(ByteString.copyFrom(plaintextParquetBytes())))

    val blob = ParquetStorageClient(blobs).getBlob("data.parquet")!!
    blob.readRows().toList() // force temp file creation
    assertThat(countTempFiles()).isGreaterThan(before)

    blob.close()
    assertThat(countTempFiles()).isEqualTo(before)
  }

  @Test
  fun `multiple reads share one temp file`(): Unit = runBlocking {
    val before = countTempFiles()
    val blobs = InMemoryStorageClient()
    blobs.writeBlob(
      "data.parquet",
      flowOf(ByteString.copyFrom(plaintextParquetBytes(metadata = mapOf("foo" to "bar")))),
    )

    ParquetStorageClient(blobs).getBlob("data.parquet")!!.use { blob ->
      blob.readRows().toList()
      blob.readKeyValueMetadata()
      blob.readRows().toList()
      // Single temp file created across three reads.
      assertThat(countTempFiles() - before).isEqualTo(1)
    }
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

  private fun sampleRows(schema: MessageType): List<org.apache.parquet.example.data.Group> =
    listOf(
      groupOf(schema) {
        add("id", 1L)
        add("name", "alice")
        add("data", org.apache.parquet.io.api.Binary.fromConstantByteArray(byteArrayOf(0xAB.toByte())))
      },
      groupOf(schema) {
        add("id", 2L)
        add("name", "bob")
        add("data", org.apache.parquet.io.api.Binary.fromConstantByteArray(byteArrayOf(0xCD.toByte())))
      },
      groupOf(schema) {
        add("id", 3L)
        add("name", "carol")
        add("data", org.apache.parquet.io.api.Binary.fromConstantByteArray(byteArrayOf(0xEF.toByte())))
      },
    )

  private fun plaintextParquetBytes(metadata: Map<String, String> = emptyMap()): ByteArray =
    writeParquet(SAMPLE_SCHEMA, sampleRows(SAMPLE_SCHEMA), null, metadata)

  private fun pmeParquetBytes(
    footerKey: ByteArray,
    metadata: Map<String, String>,
  ): ByteArray {
    val props =
      FileEncryptionProperties.builder(footerKey).withPlaintextFooter().build()
    return writeParquet(SAMPLE_SCHEMA, sampleRows(SAMPLE_SCHEMA), props, metadata)
  }

  private fun writeParquet(
    schema: MessageType,
    rows: List<org.apache.parquet.example.data.Group>,
    encryption: FileEncryptionProperties?,
    metadata: Map<String, String>,
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
      builder.build().use { writer -> rows.forEach { writer.write(it) } }
      return Files.readAllBytes(path)
    } finally {
      Files.deleteIfExists(path)
    }
  }

  private fun groupOf(
    schema: MessageType,
    block: org.apache.parquet.example.data.Group.() -> Unit,
  ): org.apache.parquet.example.data.Group {
    val g = SimpleGroupFactory(schema).newGroup()
    g.block()
    return g
  }

  private fun randomAesKey(bytes: Int): ByteArray {
    val k = ByteArray(bytes)
    SecureRandom().nextBytes(k)
    return k
  }

  private fun countTempFiles(): Int {
    val dir = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"))
    if (!Files.exists(dir)) return 0
    return Files.list(dir).use { stream ->
      stream
        .filter { it.fileName.toString().startsWith("parquet-storage-client-") }
        .count()
        .toInt()
    }
  }
}

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

import com.google.protobuf.ByteString
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.apache.parquet.crypto.FileDecryptionProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.format.Util
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.jetbrains.annotations.BlockingExecutor

/**
 * A [StorageClient] wrapper that exposes parquet-aware reads — one row per
 * emission, returned as a `Map<String, Any?>` keyed by parquet column name.
 *
 * ```
 *   underlying StorageClient (plaintext or any other decryption wrapper)
 *     wrapped by ParquetStorageClient
 *       getBlob -> ParquetBlob
 *         readRows()              -> Flow<Map<String, Any?>>   (column data)
 *         readKeyValueMetadata()  -> Map<String, String>       (plaintext footer)
 *         close()                 -> deletes the on-disk temp file
 * ```
 *
 * ## What this client deliberately is NOT
 *
 * Type-agnostic by design: no target proto descriptor, no field mapping,
 * no support for nested or repeated columns. Callers project each row
 * `Map` into their own shape (a proto, a domain object). Keeping this
 * surface narrow means the library has no opinion about the caller's
 * schema or mapping conventions and stays a thin pass-through over
 * parquet's own readers.
 *
 * Repeated fields and nested (group-typed) fields are rejected at row time
 * with [IllegalStateException]. See [ParquetBlob.readRows] for the
 * supported primitive types.
 *
 * ## Streaming I/O — why a temp file
 *
 * Parquet's footer lives at the END of the file: a reader must seek to
 * the end, read the footer length, read the footer thrift struct, then
 * seek back to row-group offsets to decode pages. There is no way to
 * read a parquet file without a seekable input.
 *
 * Two viable seekable-input backings for a remote blob:
 *  1. **In-memory `byte[]`** — load the whole blob into a `byte[]`, wrap
 *     in a `ByteArrayInputFile`. Simple but capped at ~2 GB
 *     (`Integer.MAX_VALUE` JVM array limit) and pressures heap with the
 *     full blob size.
 *  2. **On-disk temp file (this implementation)** — stream the blob bytes
 *     into a `Files.createTempFile(...)` via NIO [FileChannel] as they
 *     arrive, then hand the temp-file path to parquet's official
 *     [LocalInputFile] (which uses `FileChannel` internally for
 *     random-access reads). Disk-bound (terabytes), not heap-bound;
 *     bytes never accumulate in JVM heap. Requires writable local disk
 *     and caller-driven cleanup via [ParquetBlob.close].
 *
 * We pick (2) — (1) caps file size at 2 GB AND inflates heap pressure
 * proportional to blob size. (2) leverages parquet's own
 * [LocalInputFile] and requires no new common-jvm APIs.
 *
 * A future, fully-network-streamed design would back the
 * `SeekableInputStream` with HTTP range GETs directly against GCS / S3.
 * That requires adding a `readRange(offset, length)` primitive to
 * [StorageClient.Blob] (which doesn't exist today) and a buffered
 * range-read `SeekableInputStream`. Out of scope here.
 *
 * ## Parquet Modular Encryption (PME) — why caller-supplied, not Tink
 *
 * PME is parquet's native column-level encryption (AES-GCM, page-level).
 * Files written with PME in `PLAINTEXT_FOOTER` mode have their schema and
 * key-value metadata readable WITHOUT any decryption setup — that's the
 * bootstrap point that lets the caller pull an encrypted DEK out of the
 * footer, unwrap it via KMS, and supply column-decryption material.
 *
 * PME's [FileDecryptionProperties.Builder.withFooterKey] requires **raw
 * AES key bytes** (validated as length 16/24/32; see
 * `FileDecryptionProperties.java:51`). The existing
 * `org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption` path
 * — used by ResultsFulfiller today for whole-blob streaming-AEAD
 * decryption — produces a Tink `KeysetHandle` and exposes only Tink
 * primitives (`StreamingAead` / `Aead`). It deliberately hides raw key
 * bytes; extracting them means reaching into `KeyData.value` and parsing
 * Tink-internal protos, which Tink discourages. Additionally, Tink's
 * `AesGcmHkdfStreamingKey` is algorithmically incompatible with PME (PME
 * uses plain page-level AES-GCM; no HKDF segment derivation).
 *
 * So PME cannot reuse `withEnvelopeEncryption`. What it CAN reuse is the
 * KMS access layer (`KmsClient` factories such as
 * `GCloudKmsClientFactory` and `GCloudToAwsKmsClientFactory`, plus
 * `kmsClient.getAead(kekUri)` — generic Tink-KMS primitives that apply
 * unchanged).
 *
 * Rather than baking any of this into the library, this client takes an
 * optional [decryptionPropertiesProvider] callback. The caller — which
 * already owns the `KmsClient`, knows where in the footer the encrypted
 * DEK lives, and knows how it's serialized — implements the closure:
 *
 *  1. `blob.readKeyValueMetadata()` (no decryption needed — see below)
 *  2. read the encrypted-DEK entry + kek-URI entry from the returned map
 *  3. `kmsClient.getAead(kekUri).decrypt(encryptedDekBytes, aad)`
 *     -> raw AES bytes
 *  4. `FileDecryptionProperties.builder().withFooterKey(rawBytes).build()`
 *
 * The library stays generic: no KMS dependency, no opinion on
 * footer-key naming, no opinion on DEK serialization. The callback is
 * invoked AT MOST ONCE per [ParquetBlob] (cached after first row read).
 *
 * Write side (not implemented here): producers should use parquet-mr's
 * `FileEncryptionProperties.builder(footerKey).withPlaintextFooter().build()`
 * with `AES_GCM_V1` and stash the encrypted DEK + KEK URI as plaintext
 * key-value entries in the parquet footer.
 *
 * ### Bootstrap without keys: how [readKeyValueMetadata] reads PME footers
 *
 * Under PME `PLAINTEXT_FOOTER` mode, the FOOTER BODY (schema, key-value
 * metadata) is plaintext on disk, BUT per-column metadata is still
 * encrypted with the footer key. Parquet-mr's high-level
 * [org.apache.parquet.hadoop.ParquetFileReader] decrypts column metadata
 * eagerly when opening the file — meaning a naive
 * `ParquetFileReader.open` requires the footer key here, defeating the
 * bootstrap.
 *
 * To make [readKeyValueMetadata] work WITHOUT any key bootstrap
 * material, this implementation bypasses `ParquetFileReader` for that
 * one call and reads the raw thrift `FileMetaData` struct directly via
 * `parquet-format-structures`' `Util.readFileMetaData(in, skipRowGroups =
 * true)`. With `skipRowGroups = true`, the per-row-group / per-column
 * portions (including the encrypted per-column-metadata blobs) are
 * skipped during deserialization. Only file-level fields — including
 * the always-plaintext `key_value_metadata` — are returned.
 *
 * [readRows] still uses the high-level reader (and so still requires
 * the caller's [decryptionPropertiesProvider] for PME blobs), since
 * that's where column data actually has to be decrypted.
 *
 * ## Resource lifecycle
 *
 * Each [ParquetBlob] holds a local temp file from the first read
 * onward. The temp file is deleted by [ParquetBlob.close]. Callers MUST
 * close — use Kotlin's `.use { }` idiom:
 *
 * ```kotlin
 * parquetClient.getBlob(uri)?.use { blob ->
 *   blob.readRows().collect { row -> ... }
 * }
 * ```
 *
 * Multiple reads on the same [ParquetBlob] share one temp file
 * (downloaded once on first read, reused thereafter).
 *
 * @param storageClient underlying client for blob bytes.
 * @param parquetContext blocking context for parquet decode + temp-file
 *   download (default [Dispatchers.IO]).
 * @param decryptionPropertiesProvider optional per-blob PME callback.
 *   `null` = treat all blobs as plaintext. Invoked at most once per blob.
 */
class ParquetStorageClient(
  private val storageClient: StorageClient,
  private val parquetContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  private val decryptionPropertiesProvider:
    (suspend (ParquetBlob) -> FileDecryptionProperties?)? =
    null,
) : StorageClient {

  /**
   * A [StorageClient.Blob] that exposes parquet-aware reads + cleanup of
   * the local temp file backing the random-access reader.
   */
  interface ParquetBlob : StorageClient.Blob, AutoCloseable {
    /**
     * Cold flow of rows. Each emission is one row, represented as a
     * `Map<column name, value>`. Supported primitive types:
     *
     *  - parquet `INT32`   -> Kotlin [Int]
     *  - parquet `INT64`   -> Kotlin [Long]
     *  - parquet `FLOAT`   -> Kotlin [Float]
     *  - parquet `DOUBLE`  -> Kotlin [Double]
     *  - parquet `BOOLEAN` -> Kotlin [Boolean]
     *  - parquet `BINARY` with `STRING` logical type -> Kotlin [String]
     *  - parquet `BINARY` (no logical type) -> [ByteString]
     *  - parquet `FIXED_LEN_BYTE_ARRAY` -> [ByteString]
     *  - parquet `INT96` (legacy timestamps) -> [ByteString] (raw 12 bytes)
     *
     * OPTIONAL columns with no value present in a row map to `null`.
     * REPEATED columns (count > 1) and nested group-typed columns throw
     * [IllegalStateException] at row time.
     */
    fun readRows(): Flow<Map<String, Any?>>

    /**
     * Parquet file's footer key-value metadata. Reads the plaintext
     * footer body directly from the thrift `FileMetaData` struct without
     * involving the high-level reader, so it works on both plaintext AND
     * PME-with-plaintext-footer blobs without any decryption setup —
     * making it suitable as a bootstrap step for the caller's
     * [decryptionPropertiesProvider].
     */
    suspend fun readKeyValueMetadata(): Map<String, String>

    /** Deletes the local temp file backing this blob's parquet reads. */
    override fun close()
  }

  // === StorageClient ===

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
    storageClient.writeBlob(blobKey, content)

  override suspend fun getBlob(blobKey: String): ParquetBlob? {
    val raw = storageClient.getBlob(blobKey) ?: return null
    return ParquetBlobImpl(raw)
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
    storageClient.listBlobs(prefix)

  // === ParquetBlob impl ===

  private inner class ParquetBlobImpl(private val delegate: StorageClient.Blob) : ParquetBlob {
    override val storageClient: StorageClient = this@ParquetStorageClient
    override val blobKey: String
      get() = delegate.blobKey

    override val size: Long
      get() = delegate.size

    override val createTime: Instant
      get() = delegate.createTime

    override val updateTime: Instant
      get() = delegate.updateTime

    override fun read(): Flow<ByteString> = delegate.read()

    override suspend fun delete() = delegate.delete()

    // === Temp-file backing for parquet's random-access reads ===

    @Volatile private var tempFile: Path? = null
    private val tempFileMutex = Mutex()

    private suspend fun ensureTempFile(): Path {
      tempFile?.let { return it }
      return tempFileMutex.withLock {
        tempFile?.let { return@withLock it }
        val path = Files.createTempFile("parquet-storage-client-", ".parquet")
        try {
          Files.newByteChannel(path, StandardOpenOption.WRITE).use { ch ->
            delegate.read().collect { chunk -> ch.write(chunk.asReadOnlyByteBuffer()) }
          }
        } catch (t: Throwable) {
          try {
            Files.deleteIfExists(path)
          } catch (_: IOException) {}
          throw t
        }
        tempFile = path
        path
      }
    }

    // === Cached PME decryption properties (resolved at most once per blob) ===

    @Volatile private var decryptionResolved: Boolean = false
    @Volatile private var decryptionProps: FileDecryptionProperties? = null
    private val decryptionMutex = Mutex()

    private suspend fun resolveDecryption(): FileDecryptionProperties? {
      val provider = decryptionPropertiesProvider ?: return null
      if (decryptionResolved) return decryptionProps
      return decryptionMutex.withLock {
        if (decryptionResolved) return@withLock decryptionProps
        decryptionProps = provider.invoke(this)
        decryptionResolved = true
        decryptionProps
      }
    }

    // === Reads ===

    override fun readRows(): Flow<Map<String, Any?>> =
      flow {
          val path = ensureTempFile()
          val decryption = resolveDecryption()
          val inputFile: InputFile = LocalInputFile(path)
          GroupParquetReaderBuilder(inputFile, decryption).build().use { reader ->
            val first = reader.read() ?: return@use
            val topLevelFields = first.type.fields
            emit(groupToMap(first, topLevelFields))
            var group: Group? = reader.read()
            while (group != null) {
              emit(groupToMap(group, topLevelFields))
              group = reader.read()
            }
          }
        }
        .flowOn(parquetContext)

    override suspend fun readKeyValueMetadata(): Map<String, String> =
      withContext(parquetContext) { readFooterKeyValueMetadata(ensureTempFile()) }

    override fun close() {
      val path = tempFile ?: return
      tempFile = null
      try {
        Files.deleteIfExists(path)
      } catch (_: IOException) {
        // Best-effort. Temp file lives under java.io.tmpdir which is
        // typically cleaned by the OS / sandbox lifecycle.
      }
    }
  }

  // === Per-row conversion: Group -> Map<String, Any?> ===

  private fun groupToMap(group: Group, fields: List<Type>): Map<String, Any?> {
    val out = LinkedHashMap<String, Any?>(fields.size)
    for (field in fields) {
      val name = field.name
      if (!field.isPrimitive) {
        throw IllegalStateException(
          "Nested message field '$name' is not supported by ParquetStorageClient"
        )
      }
      val count = group.getFieldRepetitionCount(name)
      if (count == 0) {
        out[name] = null
        continue
      }
      if (count > 1) {
        throw IllegalStateException(
          "Repeated field '$name' (count=$count) is not supported by ParquetStorageClient"
        )
      }
      val prim = field.asPrimitiveType()
      out[name] =
        when (prim.primitiveTypeName) {
          PrimitiveTypeName.INT32 -> group.getInteger(name, 0)
          PrimitiveTypeName.INT64 -> group.getLong(name, 0)
          PrimitiveTypeName.FLOAT -> group.getFloat(name, 0)
          PrimitiveTypeName.DOUBLE -> group.getDouble(name, 0)
          PrimitiveTypeName.BOOLEAN -> group.getBoolean(name, 0)
          PrimitiveTypeName.BINARY -> {
            val binary = group.getBinary(name, 0)
            val annotation = prim.logicalTypeAnnotation
            // Treat BINARY as String iff annotated as UTF-8 (STRING /
            // ENUM logical types). Otherwise hand back raw bytes.
            if (annotation != null && annotation.toString().startsWith("STRING")) {
              binary.toStringUsingUTF8()
            } else {
              ByteString.copyFrom(binary.toByteBuffer())
            }
          }
          PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ->
            ByteString.copyFrom(group.getBinary(name, 0).toByteBuffer())
          PrimitiveTypeName.INT96 -> ByteString.copyFrom(group.getInt96(name, 0).bytes)
          else ->
            throw IllegalStateException(
              "Unsupported primitive type '${prim.primitiveTypeName}' for field '$name'"
            )
        }
    }
    return out
  }

  // === Parquet plumbing ===

  /**
   * Subclass of [ParquetReader.Builder] needed to reach the protected
   * `Builder(InputFile)` constructor — parquet-mr's public static factory
   * `builder()` only accepts a Hadoop `Path`. If [decryption] is
   * non-null, install it via the parquet-mr `withDecryption` builder API,
   * which flows into the eventual `ParquetReadOptions` and enables PME
   * column decryption.
   */
  private class GroupParquetReaderBuilder(
    file: InputFile,
    decryption: FileDecryptionProperties?,
  ) : ParquetReader.Builder<Group>(file) {
    init {
      decryption?.let { withDecryption(it) }
    }

    override fun getReadSupport(): ReadSupport<Group> = GroupReadSupport()
  }

  // === Direct thrift footer parse (bypasses ParquetFileReader) ===

  /**
   * Reads the parquet file's footer key-value metadata directly from
   * the thrift `FileMetaData` struct, bypassing parquet-mr's high-level
   * [org.apache.parquet.hadoop.ParquetFileReader].
   *
   * Why bypass: under PME `PLAINTEXT_FOOTER` mode the footer body
   * (schema, key-value metadata) IS plaintext on disk, but per-column
   * metadata is still encrypted with the footer key.
   * `ParquetFileReader.open` decrypts ALL column metadata eagerly when
   * opening the file, which requires the footer key here — defeating
   * the whole point of reading the footer first to obtain key
   * bootstrap material.
   *
   * Solution: read the raw FileMetaData thrift struct via
   * `parquet-format-structures`' `Util.readFileMetaData(in,
   * skipRowGroups = true)`. With `skipRowGroups = true`, the
   * per-row-group / per-column portions are skipped during
   * deserialization, so the encrypted per-column-metadata blobs are
   * never touched. Only file-level fields, including the
   * always-plaintext `key_value_metadata`, are returned.
   *
   * Parquet file footer layout (last bytes):
   * ```
   *   ...FileMetaData (thrift) bytes...
   *   [int32 footer length LE][4-byte magic]
   * ```
   * Magic is `"PAR1"` for plaintext-footer files and `"PARE"` for
   * encrypted-footer PME files. We accept both — the FileMetaData
   * thrift itself is still parseable for `PARE` files as long as we
   * use `skipRowGroups = true` (per-column encryption never engages).
   */
  private fun readFooterKeyValueMetadata(path: Path): Map<String, String> {
    FileChannel.open(path, StandardOpenOption.READ).use { ch ->
      val fileLen = ch.size()
      require(fileLen >= MIN_PARQUET_FILE_SIZE) {
        "File too small to be parquet: $path (size=$fileLen)"
      }
      // Read the 8-byte trailer: [footer length: int32 LE][magic: 4 bytes].
      val tail = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      ch.position(fileLen - 8)
      readFully(ch, tail)
      tail.flip()
      val footerLen = tail.int
      val magicBytes = ByteArray(4).also { tail.get(it) }
      val magic = String(magicBytes, Charsets.US_ASCII)
      require(magic == PARQUET_MAGIC_PLAINTEXT || magic == PARQUET_MAGIC_ENCRYPTED_FOOTER) {
        "Not a parquet file: $path (bad magic '$magic')"
      }
      require(footerLen in 1..(fileLen - 8)) {
        "Implausible parquet footer length $footerLen for file size $fileLen"
      }
      val footerStart = fileLen - 8 - footerLen
      val footerBuf = ByteBuffer.allocate(footerLen)
      ch.position(footerStart)
      readFully(ch, footerBuf)
      footerBuf.flip()
      val footerBytes = ByteArray(footerLen).also { footerBuf.get(it) }
      val md = Util.readFileMetaData(ByteArrayInputStream(footerBytes), /* skipRowGroups = */ true)
      val kv = md.key_value_metadata ?: return emptyMap()
      return kv.associate { it.key to (it.value ?: "") }
    }
  }

  private fun readFully(ch: FileChannel, buf: ByteBuffer) {
    while (buf.hasRemaining()) {
      val n = ch.read(buf)
      if (n < 0) throw EOFException("Unexpected EOF reading parquet bytes")
    }
  }

  private companion object {
    private const val PARQUET_MAGIC_PLAINTEXT = "PAR1"
    private const val PARQUET_MAGIC_ENCRYPTED_FOOTER = "PARE"
    private const val MIN_PARQUET_FILE_SIZE = 12L // 4-byte header + 8-byte trailer minimum.
  }
}

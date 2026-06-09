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
import java.nio.channels.SeekableByteChannel
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
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.jetbrains.annotations.BlockingExecutor

/**
 * A [StorageClient] wrapper that exposes parquet-aware reads.
 *
 * Each [getBlob] returns a [ParquetBlob] with:
 *  - [ParquetBlob.readRows] — `Flow<Map<String, Any?>>`, one row per
 *    emission, keyed by parquet column name with native-typed values.
 *  - [ParquetBlob.readKeyValueMetadata] — the file's footer key-value
 *    metadata as a `Map<String, String>`.
 *
 * ## Usage
 *
 * ```kotlin
 * parquetClient.getBlob(uri)?.use { blob ->
 *   blob.readRows().collect { row -> ... }
 * }
 * ```
 *
 * Each [getBlob] returns a FRESH [ParquetBlob] that downloads the
 * underlying blob to a local temp file on first read; multiple reads on
 * the same instance share that temp file. Callers MUST close
 * (the `.use { }` idiom handles this); read methods on a closed blob
 * throw [IllegalStateException].
 *
 * ## Constraints
 *
 * - Nested (group-typed) and REPEATED columns throw
 *   [IllegalStateException] at row time. This client is single-value,
 *   flat-schema only.
 * - See [ParquetBlob.readRows] for the supported primitive types and
 *   how they map to Kotlin/Java values.
 *
 * ## Parquet Modular Encryption (PME)
 *
 * For PME-encrypted blobs, supply [decryptionPropertiesProvider]. The
 * callback runs at most once per blob and MAY call
 * [ParquetBlob.readKeyValueMetadata] on the same blob (the typical
 * bootstrap: read an encrypted DEK + KEK URI out of the footer, unwrap
 * via KMS, return [org.apache.parquet.crypto.FileDecryptionProperties]).
 * It MUST NOT call [ParquetBlob.readRows] on the same blob — that
 * would deadlock on the resolution mutex.
 *
 * Only `PLAINTEXT_FOOTER` mode is supported; `ENCRYPTED_FOOTER` blobs
 * are rejected with a clear error at read time.
 *
 * @param storageClient underlying client for raw blob bytes.
 * @param parquetContext blocking context for parquet decode + temp-file
 *   download (default [Dispatchers.IO]).
 * @param decryptionPropertiesProvider optional per-blob PME callback;
 *   `null` = treat all blobs as plaintext.
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
     *  - parquet `BINARY` with `STRING` / `ENUM` / `JSON` logical type
     *    -> Kotlin [String] (UTF-8 decoded)
     *  - parquet `BINARY` with any other (or no) logical type
     *    -> [ByteString] (raw bytes; this covers `BSON`, `UUID`, raw
     *    bytes columns, etc.)
     *  - parquet `FIXED_LEN_BYTE_ARRAY` -> [ByteString]
     *  - parquet `INT96` (legacy timestamps) -> [ByteString] (raw 12 bytes)
     *
     * OPTIONAL columns with no value present in a row map to `null`.
     * REPEATED columns (count > 1) and nested group-typed columns throw
     * [IllegalStateException] at row time.
     *
     * Throws [IllegalStateException] if the blob has been [close]d.
     */
    fun readRows(): Flow<Map<String, Any?>>

    /**
     * Parquet file's footer key-value metadata. Reads the plaintext
     * footer body directly from the thrift `FileMetaData` struct without
     * involving the high-level reader, so it works on both plaintext AND
     * PME-with-`PLAINTEXT_FOOTER` blobs without any decryption setup —
     * making it suitable as a bootstrap step for the caller's
     * `decryptionPropertiesProvider`.
     *
     * Throws [IllegalStateException] if the blob has been [close]d, or
     * the file is a PME `ENCRYPTED_FOOTER` blob (not supported).
     */
    suspend fun readKeyValueMetadata(): Map<String, String>

    /**
     * Deletes the local temp file backing this blob's parquet reads.
     * After `close()`, all read methods throw [IllegalStateException].
     * Idempotent — calling `close()` multiple times is safe.
     */
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
    @Volatile private var closed: Boolean = false
    private val tempFileMutex = Mutex()

    private fun checkOpen() {
      check(!closed) { "ParquetBlob '$blobKey' has been closed" }
    }

    private suspend fun ensureTempFile(): Path {
      tempFile?.let { return it }
      return tempFileMutex.withLock {
        tempFile?.let { return@withLock it }
        val path = Files.createTempFile("parquet-storage-client-", ".parquet")
        try {
          Files.newByteChannel(path, StandardOpenOption.WRITE).use { ch ->
            delegate.read().collect { chunk -> writeFully(ch, chunk.asReadOnlyByteBuffer()) }
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

    /**
     * Holds [decryptionMutex] while invoking the caller-supplied
     * provider. The provider may call [readKeyValueMetadata] on the
     * same blob (different mutex) but MUST NOT call [readRows] on the
     * same blob — that would re-enter this method and deadlock on the
     * non-reentrant coroutine `Mutex`. See class KDoc.
     */
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
          checkOpen()
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
      withContext(parquetContext) {
        checkOpen()
        readFooterKeyValueMetadata(ensureTempFile())
      }

    override fun close() {
      if (closed) return
      closed = true
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
            // STRING / ENUM / JSON are UTF-8 text logical types -> decode
            // to String. Everything else (including BSON, UUID, raw
            // bytes, or no annotation) stays as raw bytes.
            when (prim.logicalTypeAnnotation) {
              is LogicalTypeAnnotation.StringLogicalTypeAnnotation,
              is LogicalTypeAnnotation.EnumLogicalTypeAnnotation,
              is LogicalTypeAnnotation.JsonLogicalTypeAnnotation -> binary.toStringUsingUTF8()
              else -> ByteString.copyFrom(binary.toByteBuffer())
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
   * Magic is `"PAR1"` for plaintext-footer files (including PME with
   * `PLAINTEXT_FOOTER`). `"PARE"` indicates PME `ENCRYPTED_FOOTER`
   * mode, which we explicitly REJECT here: in that mode the footer body
   * is a `FileCryptoMetaData` thrift followed by the encrypted
   * `FileMetaData`, so `Util.readFileMetaData` would hit ciphertext and
   * throw an obscure thrift parse error. The bootstrap design built
   * around this class requires `PLAINTEXT_FOOTER`; fail fast with a
   * clear message instead.
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
      if (magic == PARQUET_MAGIC_ENCRYPTED_FOOTER) {
        throw IllegalStateException(
          "Parquet file '$path' uses PME ENCRYPTED_FOOTER mode, which is not supported by " +
            "ParquetStorageClient. Re-write the file with PLAINTEXT_FOOTER mode " +
            "(FileEncryptionProperties.Builder.withPlaintextFooter()) so the footer key-value " +
            "metadata can be read without the footer key."
        )
      }
      require(magic == PARQUET_MAGIC_PLAINTEXT) {
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

  /**
   * Loops until the channel has consumed all bytes in [buf].
   * `SeekableByteChannel.write` is contractually allowed to return a
   * short write; on local files the JDK almost always writes fully in
   * one call, but the spec doesn't guarantee it. A silent short-write
   * here would corrupt the temp file (parquet footer at the wrong
   * offset), so we explicitly loop.
   */
  private fun writeFully(ch: SeekableByteChannel, buf: ByteBuffer) {
    while (buf.hasRemaining()) {
      ch.write(buf)
    }
  }

  private companion object {
    private const val PARQUET_MAGIC_PLAINTEXT = "PAR1"
    private const val PARQUET_MAGIC_ENCRYPTED_FOOTER = "PARE"
    private const val MIN_PARQUET_FILE_SIZE = 12L // 4-byte header + 8-byte trailer minimum.
  }
}

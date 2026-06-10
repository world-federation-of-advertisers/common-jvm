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

import com.google.crypto.tink.KmsClient
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.type.Date
import java.io.ByteArrayInputStream
import java.io.FileNotFoundException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.util.Base64
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.crypto.FileDecryptionProperties
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.format.Util
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.jetbrains.annotations.BlockingExecutor

/**
 * Declarative PME read-decryption config for the standard envelope-encryption
 * convention: the parquet footer key-value metadata carries a KEK URI and a
 * base64-encoded, KMS-wrapped DEK. The client reads the footer, unwraps the
 * DEK via [kmsClient], and uses it as the parquet footer key — no caller code.
 *
 * Single-DEK only (one footer key for the whole file), matching the EDP
 * convention. Per-column keys are not supported.
 *
 * @param kmsClient KMS client used to unwrap the DEK via `getAead(kekUri)`.
 * @param kekUriMetadataKey footer metadata key holding the KEK URI.
 * @param encryptedDekMetadataKey footer metadata key holding the base64-encoded
 *   KMS-wrapped DEK.
 */
data class ParquetDecryptionConfig(
  val kmsClient: KmsClient,
  val kekUriMetadataKey: String = "edpa.kek_uri",
  val encryptedDekMetadataKey: String = "edpa.encrypted_dek",
)

/**
 * A [StorageClient] backed by a Hadoop [Configuration] that reads and writes
 * parquet blobs.
 *
 * The [Configuration] selects the storage backend through Hadoop's
 * `FileSystem` abstraction, so the same client transparently reads from
 * cloud or local storage depending on the [blobKey][StorageClient.Blob.blobKey]
 * scheme:
 *  - `gs://…` — Google Cloud Storage (requires the `gcs-connector` runtime
 *    dependency and `fs.gs.impl` configured to `GoogleHadoopFileSystem`).
 *  - `file://…` / relative — the local filesystem (works with no extra
 *    dependency; used by unit tests).
 *
 * All blob keys are resolved relative to [rootPath]. Reads are performed as
 * random-access range reads through the backend connector (parquet seeks to
 * the footer, then to the row groups it needs) — nothing is staged on local
 * disk, so reads scale to the backend's object-size limits.
 *
 * ## Two read APIs
 *
 * [getBlob] returns a [ParquetBlob] exposing:
 *  - [ParquetBlob.readRows] — `Flow<Map<String, Any?>>`, one map per row,
 *    keyed by parquet column name with native-typed values (see
 *    [ParquetBlob.readRows] for the type mapping).
 *  - [ParquetBlob.readKeyValueMetadata] — the file's footer key-value
 *    metadata as a `Map<String, String>`.
 *
 * The base [StorageClient.Blob.read] / [writeBlob] pair speaks a row-proto
 * codec: each `ByteString` in the flow is one serialized [ParquetRow]. This
 * makes the [StorageClient] contract parquet-aware on both ends — `writeBlob`
 * encodes rows into a parquet file, `read` decodes them back — but note it is
 * NOT the opaque byte pass-through that the other [StorageClient]
 * implementations provide; the unit of content here is a row, not a file.
 *
 * ## Type mapping
 *
 * See [ParquetRow]/[ParquetValue] for the full parquet ⇄ proto ⇄ Kotlin type
 * table. Reads collapse three physical byte types to `bytes` and three
 * timestamp precisions to `timestamp`; writes pick a canonical physical type
 * per value, so round-trips are value-preserving, not parquet-physical-byte
 * preserving.
 *
 * ## Constraints
 *
 * - Nested (group-typed) and REPEATED columns throw [IllegalStateException]
 *   at row time. This client is single-value, flat-schema only.
 * - [writeBlob] derives the parquet schema from the first [ParquetRow]; the
 *   first row MUST set every column (no `KIND_NOT_SET`/null values) so each
 *   column's type can be inferred. Subsequent rows may omit columns (NULL).
 * - [writeBlob] writes plaintext parquet unless [encryptionPropertiesProvider]
 *   is supplied. In production the EDP path writes encrypted files elsewhere,
 *   so write-side PME is mainly for symmetry / round-trip testing.
 *
 * ## Parquet Modular Encryption (PME)
 *
 * For PME-encrypted blobs following the standard envelope-encryption
 * convention (footer metadata carries a KEK URI + a KMS-wrapped DEK), supply
 * [decryptionConfig]: the client reads the footer, unwraps the DEK via the
 * configured KMS, and uses it as the footer key — no caller code. See
 * [ParquetDecryptionConfig]. `null` config = plaintext reads.
 *
 * `ENCRYPTED_FOOTER` blobs are not supported by [readKeyValueMetadata]
 * (rejected with a clear error). The bootstrap pattern this class is built
 * around requires `PLAINTEXT_FOOTER`; writers SHOULD use `PLAINTEXT_FOOTER`
 * mode.
 *
 * @param conf Hadoop configuration selecting the storage backend.
 * @param rootPath base path; all blob keys are resolved relative to it.
 * @param parquetContext blocking context for parquet decode/encode + the
 *   backend FileSystem calls (default [Dispatchers.IO]).
 * @param decryptionConfig optional declarative PME read config for the
 *   standard KEK-URI + wrapped-DEK convention; `null` = plaintext reads.
 * @param encryptionPropertiesProvider optional per-blob-key PME write
 *   callback resolved once per [writeBlob]; return `null` (or omit) to write
 *   plaintext. Use `PLAINTEXT_FOOTER` mode if the file's footer key-value
 *   metadata must remain readable for the read-side bootstrap.
 */
class ParquetStorageClient(
  private val conf: Configuration,
  rootPath: Path,
  private val parquetContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
  private val decryptionConfig: ParquetDecryptionConfig? = null,
  private val encryptionPropertiesProvider:
    (suspend (String) -> FileEncryptionProperties?)? =
    null,
) : StorageClient {

  private val fileSystem: FileSystem = rootPath.getFileSystem(conf)
  private val qualifiedRoot: Path = fileSystem.makeQualified(rootPath)

  // Base URI for relativizing listed paths back to blob keys; computed once
  // (must end in "/" for URI.relativize to treat it as a directory).
  private val rootUri: URI = URI.create(qualifiedRoot.toUri().toString().trimEnd('/') + "/")

  /** A [StorageClient.Blob] that exposes parquet-aware reads. */
  interface ParquetBlob : StorageClient.Blob {
    /**
     * Cold flow of rows. Each emission is one row, represented as a
     * `Map<column name, value>`. Supported primitive types:
     *
     *  - `INT32`                       -> [Int]
     *  - `INT32` + `DATE`              -> [java.time.LocalDate]
     *  - `INT64`                       -> [Long]
     *  - `INT64` + `TIMESTAMP_*`       -> [java.time.Instant]
     *  - `FLOAT`                       -> [Float]
     *  - `DOUBLE`                      -> [Double]
     *  - `BOOLEAN`                     -> [Boolean]
     *  - `BINARY` + `STRING`/`ENUM`/`JSON` -> [String] (UTF-8 decoded)
     *  - `BINARY` (any other / none)   -> [ByteString] (raw bytes; covers
     *    `BSON`, `UUID`, raw bytes columns, etc.)
     *  - `FIXED_LEN_BYTE_ARRAY`        -> [ByteString]
     *  - `INT96` (legacy timestamps)   -> [ByteString] (raw 12 bytes)
     *
     * OPTIONAL columns with no value present in a row map to `null`.
     * REPEATED columns (count > 1) and nested group-typed columns throw
     * [IllegalStateException] at row time.
     */
    fun readRows(): Flow<Map<String, Any?>>

    /**
     * Parquet file's footer key-value metadata. Reads the plaintext footer
     * body directly from the thrift `FileMetaData` struct without involving
     * the high-level reader, so it works on both plaintext AND
     * PME-with-`PLAINTEXT_FOOTER` blobs without any decryption setup — it is
     * the same footer read the client uses internally to bootstrap
     * [ParquetDecryptionConfig].
     *
     * Throws [IllegalStateException] if the file is a PME `ENCRYPTED_FOOTER`
     * blob (not supported).
     */
    suspend fun readKeyValueMetadata(): Map<String, String>
  }

  // === StorageClient ===

  /**
   * Encodes [content] (a flow of serialized [ParquetRow]) into a parquet file
   * at [blobKey]. The schema is derived from the first row; see the class
   * KDoc for the first-row constraint. An empty flow writes a valid, zero-row
   * parquet file.
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    val path = resolvePath(blobKey)
    val encryption: FileEncryptionProperties? = encryptionPropertiesProvider?.invoke(blobKey)
    withContext(parquetContext) {
      val outputFile: OutputFile = HadoopOutputFile.fromPath(path, conf)
      var writer: ParquetWriter<Group>? = null
      var factory: SimpleGroupFactory? = null
      var schema: MessageType? = null
      var expectedKinds: Map<String, ParquetValue.KindCase>? = null
      try {
        content.collect { bytes ->
          val row = ParquetRow.parseFrom(bytes)
          if (writer == null) {
            schema = deriveSchema(row)
            expectedKinds = row.columnsMap.mapValues { it.value.kindCase }
            factory = SimpleGroupFactory(schema)
            writer = newWriter(outputFile, schema!!, encryption)
          }
          validateRow(row, expectedKinds!!)
          writer!!.write(rowToGroup(row, schema!!, factory!!))
        }
        if (writer == null) {
          // Parquet rejects an empty (zero-column) schema, so empty input
          // writes a single placeholder column with zero rows; reads then
          // yield no rows.
          writer = newWriter(outputFile, EMPTY_SCHEMA, encryption)
        }
      } catch (t: Throwable) {
        // Release the open writer without masking the original failure, then
        // delete the partial object so a failed write never leaves a
        // half-written / corrupt blob behind.
        try {
          writer?.close()
        } catch (closeError: Throwable) {
          t.addSuppressed(closeError)
        }
        deleteQuietly(path)
        throw t
      }
      // Finalize: close() writes the footer. A failure here is also a failed
      // write, so clean up the partial object.
      try {
        writer!!.close()
      } catch (t: Throwable) {
        deleteQuietly(path)
        throw t
      }
    }
    return ParquetBlobImpl(blobKey, path)
  }

  private fun deleteQuietly(path: Path) {
    try {
      fileSystem.delete(path, /* recursive = */ false)
    } catch (_: Exception) {
      // Best-effort cleanup; the original failure is what matters.
    }
  }

  override suspend fun getBlob(blobKey: String): ParquetBlob? {
    val path = resolvePath(blobKey)
    return withContext(parquetContext) {
      // One metadata call that doubles as the existence check and primes the
      // blob's status cache (size/createTime/updateTime reuse it).
      val status =
        try {
          fileSystem.getFileStatus(path)
        } catch (e: FileNotFoundException) {
          return@withContext null
        }
      ParquetBlobImpl(blobKey, path, status)
    }
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
    channelFlow {
        if (fileSystem.exists(qualifiedRoot)) {
          val iter = fileSystem.listFiles(qualifiedRoot, /* recursive = */ true)
          while (iter.hasNext()) {
            val status = iter.next()
            val key = relativeKey(status.path)
            if (prefix.isNullOrEmpty() || key.startsWith(prefix)) {
              // listFiles already returned the status — reuse it instead of
              // re-fetching per metadata access.
              trySendBlocking(ParquetBlobImpl(key, status.path, status))
            }
          }
        }
      }
      .flowOn(parquetContext)

  // === ParquetBlob impl ===

  private inner class ParquetBlobImpl(
    override val blobKey: String,
    private val path: Path,
    preloadedStatus: FileStatus? = null,
  ) : ParquetBlob {
    override val storageClient: StorageClient = this@ParquetStorageClient

    // Snapshot of the file's status. Preloaded from getBlob/listBlobs when
    // available, otherwise fetched once and reused across the three metadata
    // accessors below (avoids redundant getFileStatus round trips).
    @Volatile private var cachedStatus: FileStatus? = preloadedStatus

    private fun status(): FileStatus =
      cachedStatus ?: fileSystem.getFileStatus(path).also { cachedStatus = it }

    override val size: Long
      get() = status().len

    // Hadoop FileStatus exposes only a modification time; use it for both.
    override val createTime: Instant
      get() = Instant.ofEpochMilli(status().modificationTime)

    override val updateTime: Instant
      get() = Instant.ofEpochMilli(status().modificationTime)

    /** Row-proto codec read: one serialized [ParquetRow] per parquet row. */
    override fun read(): Flow<ByteString> =
      rowFlow { group, decoders -> rowToProto(group, decoders).toByteString() }

    override fun readRows(): Flow<Map<String, Any?>> =
      rowFlow { group, decoders -> rowToNativeMap(group, decoders) }

    override suspend fun readKeyValueMetadata(): Map<String, String> =
      withContext(parquetContext) { readFooterKeyValueMetadata(path) }

    override suspend fun delete() {
      withContext(parquetContext) { fileSystem.delete(path, /* recursive = */ false) }
    }

    // === PME decryption (declarative config bootstrap) ===

    /**
     * Standard envelope-encryption bootstrap: read the plaintext footer
     * metadata, pull the KEK URI + base64 KMS-wrapped DEK from the configured
     * keys, unwrap the DEK via KMS, and use it as the parquet footer key.
     * Returns `null` when no [decryptionConfig] is set (plaintext blobs).
     *
     * Resolved fresh per read flow: a [org.apache.parquet.crypto.FileDecryptionProperties]
     * is single-use in parquet-mr, and reads are not hot enough to warrant
     * caching the KMS unwrap.
     */
    private suspend fun resolveDecryption(): FileDecryptionProperties? {
      val config = decryptionConfig ?: return null
      val metadata = readFooterKeyValueMetadata(path)
      val kekUri =
        metadata[config.kekUriMetadataKey]
          ?: throw IllegalStateException(
            "Parquet blob '$blobKey' is missing KEK URI metadata key " +
              "'${config.kekUriMetadataKey}'"
          )
      val wrappedDek =
        metadata[config.encryptedDekMetadataKey]
          ?: throw IllegalStateException(
            "Parquet blob '$blobKey' is missing encrypted-DEK metadata key " +
              "'${config.encryptedDekMetadataKey}'"
          )
      val dek = config.kmsClient.getAead(kekUri).decrypt(Base64.getDecoder().decode(wrappedDek), EMPTY_AAD)
      return FileDecryptionProperties.builder().withFooterKey(dek).build()
    }

    /**
     * Cold flow over the file's parquet rows. Opens a fresh reader per
     * collection (range reads through the backend connector), pre-compiles
     * per-column decoders once from the first row's schema, then applies
     * [transform] to each parquet [Group]. The transform decides whether to
     * materialize a [ParquetRow] proto ([read]) or go straight to a native map
     * ([readRows]) — the latter skips proto allocation entirely.
     */
    private fun <T> rowFlow(transform: (Group, List<ColumnDecoder>) -> T): Flow<T> =
      flow {
          val decryption = resolveDecryption()
          val inputFile: InputFile = HadoopInputFile.fromPath(path, conf)
          GroupParquetReaderBuilder(inputFile, decryption).build().use { reader ->
            val first = reader.read() ?: return@use
            val decoders = buildDecoders(first.type.fields)
            emit(transform(first, decoders))
            var group: Group? = reader.read()
            while (group != null) {
              emit(transform(group, decoders))
              group = reader.read()
            }
          }
        }
        .flowOn(parquetContext)
  }

  // === Path helpers ===

  private fun resolvePath(blobKey: String): Path = Path(qualifiedRoot, blobKey)

  private fun relativeKey(child: Path): String = rootUri.relativize(child.toUri()).path

  // === Read: per-column decoder compilation (runs once per file) ===

  /**
   * Per-column decoder: `(column name, (Group) -> native value or null)`. The
   * native value is the Kotlin/Java type from the [ParquetBlob.readRows] table
   * ([Int]/[Long]/[Float]/[Double]/[Boolean]/[String]/[ByteString]/[Instant]/
   * [LocalDate]), or `null` for an absent OPTIONAL column.
   */
  private class ColumnDecoder(val name: String, val extract: (Group) -> Any?)

  private fun buildDecoders(fields: List<Type>): List<ColumnDecoder> =
    fields.map { buildDecoder(it) }

  private fun buildDecoder(field: Type): ColumnDecoder {
    val name = field.name
    if (!field.isPrimitive) {
      throw IllegalStateException(
        "Nested message field '$name' is not supported by ParquetStorageClient"
      )
    }
    val valueOf: (Group) -> Any = buildPrimitiveExtractor(name, field.asPrimitiveType())
    return ColumnDecoder(name) { group ->
      when (val count = group.getFieldRepetitionCount(name)) {
        0 -> null
        1 -> valueOf(group)
        else ->
          throw IllegalStateException(
            "Repeated field '$name' (count=$count) is not supported by ParquetStorageClient"
          )
      }
    }
  }

  /**
   * Compiles the native-value extractor for a single primitive column.
   * Primitive-type dispatch and BINARY/INT32/INT64 logical-type detection
   * happen ONCE here, not per row. The `when` is exhaustive over
   * [PrimitiveTypeName] (no `else`) so a future parquet primitive surfaces as
   * a compile error rather than a silent runtime fallthrough.
   */
  private fun buildPrimitiveExtractor(name: String, prim: PrimitiveType): (Group) -> Any {
    val annotation = prim.logicalTypeAnnotation
    return when (prim.primitiveTypeName) {
      PrimitiveTypeName.INT32 ->
        if (annotation is LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
          { g -> LocalDate.ofEpochDay(g.getInteger(name, 0).toLong()) }
        } else {
          { g -> g.getInteger(name, 0) }
        }
      PrimitiveTypeName.INT64 ->
        if (annotation is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
          val unit = annotation.unit
          { g -> instantOf(g.getLong(name, 0), unit) }
        } else {
          { g -> g.getLong(name, 0) }
        }
      PrimitiveTypeName.FLOAT -> { g -> g.getFloat(name, 0) }
      PrimitiveTypeName.DOUBLE -> { g -> g.getDouble(name, 0) }
      PrimitiveTypeName.BOOLEAN -> { g -> g.getBoolean(name, 0) }
      PrimitiveTypeName.BINARY ->
        if (
          annotation is LogicalTypeAnnotation.StringLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.EnumLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.JsonLogicalTypeAnnotation
        ) {
          { g -> g.getBinary(name, 0).toStringUsingUTF8() }
        } else {
          { g -> ByteString.copyFrom(g.getBinary(name, 0).toByteBuffer()) }
        }
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> { g ->
        ByteString.copyFrom(g.getBinary(name, 0).toByteBuffer())
      }
      PrimitiveTypeName.INT96 -> { g -> ByteString.copyFrom(g.getInt96(name, 0).bytes) }
    }
  }

  /**
   * Builds a [ParquetRow] proto from a parquet [Group] (the [read] codec path).
   * Only invoked when a serialized proto is actually needed; [readRows] skips
   * this and builds a native map directly.
   */
  private fun rowToProto(group: Group, decoders: List<ColumnDecoder>): ParquetRow {
    val builder = ParquetRow.newBuilder()
    for (decoder in decoders) {
      builder.putColumns(decoder.name, valueToProto(decoder.extract(group)))
    }
    return builder.build()
  }

  /** Projects a parquet [Group] straight into a column-ordered native map. */
  private fun rowToNativeMap(group: Group, decoders: List<ColumnDecoder>): Map<String, Any?> {
    val out = LinkedHashMap<String, Any?>(decoders.size, 1f)
    for (decoder in decoders) out[decoder.name] = decoder.extract(group)
    return out
  }

  // === Write: schema derivation + Group construction ===

  private fun newWriter(
    outputFile: OutputFile,
    schema: MessageType,
    encryption: FileEncryptionProperties?,
  ): ParquetWriter<Group> {
    // ExampleParquetWriter installs a GroupWriteSupport bound to this schema,
    // so no shared-Configuration mutation is needed.
    val builder =
      ExampleParquetWriter.builder(outputFile)
        .withConf(conf)
        .withType(schema)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
    if (encryption != null) builder.withEncryption(encryption)
    return builder.build()
  }

  /**
   * Derives the parquet [MessageType] from the first [ParquetRow]. Every
   * column is OPTIONAL so later rows can omit values (NULL). Each
   * [ParquetValue.KindCase] maps to a canonical physical type. The first row
   * must set every column; a `KIND_NOT_SET` value cannot be typed and throws.
   */
  private fun deriveSchema(row: ParquetRow): MessageType {
    val fields =
      row.columnsMap.map { (name, value) ->
        when (value.kindCase) {
          ParquetValue.KindCase.INT32_VALUE ->
            Types.optional(PrimitiveTypeName.INT32).named(name)
          ParquetValue.KindCase.INT64_VALUE ->
            Types.optional(PrimitiveTypeName.INT64).named(name)
          ParquetValue.KindCase.FLOAT_VALUE ->
            Types.optional(PrimitiveTypeName.FLOAT).named(name)
          ParquetValue.KindCase.DOUBLE_VALUE ->
            Types.optional(PrimitiveTypeName.DOUBLE).named(name)
          ParquetValue.KindCase.BOOL_VALUE ->
            Types.optional(PrimitiveTypeName.BOOLEAN).named(name)
          ParquetValue.KindCase.STRING_VALUE ->
            Types.optional(PrimitiveTypeName.BINARY)
              .`as`(LogicalTypeAnnotation.stringType())
              .named(name)
          ParquetValue.KindCase.BYTES_VALUE ->
            Types.optional(PrimitiveTypeName.BINARY).named(name)
          ParquetValue.KindCase.TIMESTAMP_VALUE ->
            Types.optional(PrimitiveTypeName.INT64)
              .`as`(
                LogicalTypeAnnotation.timestampType(
                  /* isAdjustedToUTC = */ true,
                  LogicalTypeAnnotation.TimeUnit.MICROS,
                )
              )
              .named(name)
          ParquetValue.KindCase.DATE_VALUE ->
            Types.optional(PrimitiveTypeName.INT32)
              .`as`(LogicalTypeAnnotation.dateType())
              .named(name)
          ParquetValue.KindCase.KIND_NOT_SET ->
            throw IllegalArgumentException(
              "First ParquetRow column '$name' has no value; the first row must be fully " +
                "populated so the parquet schema can be derived."
            )
        }
      }
    return MessageType("ParquetRow", fields)
  }

  /**
   * Validates a row against the column→kind map derived from the first row.
   * Rejects columns absent from the first row (would be silently dropped) and
   * kind mismatches (would produce a wrong-typed parquet write). Columns the
   * row omits, or sets to `KIND_NOT_SET`, are allowed (OPTIONAL → NULL).
   */
  private fun validateRow(row: ParquetRow, expectedKinds: Map<String, ParquetValue.KindCase>) {
    for ((name, value) in row.columnsMap) {
      if (value.kindCase == ParquetValue.KindCase.KIND_NOT_SET) continue
      val expected =
        expectedKinds[name]
          ?: throw IllegalArgumentException(
            "ParquetRow column '$name' is not in the schema derived from the first row " +
              "(columns: ${expectedKinds.keys}); all rows must share the first row's columns."
          )
      if (value.kindCase != expected) {
        throw IllegalArgumentException(
          "ParquetRow column '$name' has kind ${value.kindCase} but the schema (from the first " +
            "row) expects $expected"
        )
      }
    }
  }

  private fun rowToGroup(row: ParquetRow, schema: MessageType, factory: SimpleGroupFactory): Group {
    val group = factory.newGroup()
    for (field in schema.fields) {
      val value = row.columnsMap[field.name] ?: continue
      appendToGroup(group, field.name, value)
    }
    return group
  }

  private fun appendToGroup(group: Group, name: String, value: ParquetValue) {
    when (value.kindCase) {
      ParquetValue.KindCase.INT32_VALUE -> group.add(name, value.int32Value)
      ParquetValue.KindCase.INT64_VALUE -> group.add(name, value.int64Value)
      ParquetValue.KindCase.FLOAT_VALUE -> group.add(name, value.floatValue)
      ParquetValue.KindCase.DOUBLE_VALUE -> group.add(name, value.doubleValue)
      ParquetValue.KindCase.BOOL_VALUE -> group.add(name, value.boolValue)
      ParquetValue.KindCase.STRING_VALUE -> group.add(name, value.stringValue)
      ParquetValue.KindCase.BYTES_VALUE ->
        group.add(name, Binary.fromConstantByteArray(value.bytesValue.toByteArray()))
      ParquetValue.KindCase.TIMESTAMP_VALUE -> group.add(name, instantToMicros(value.timestampValue))
      ParquetValue.KindCase.DATE_VALUE ->
        group.add(name, dateToEpochDay(value.dateValue).toInt())
      // NULL: leave the OPTIONAL column unset.
      ParquetValue.KindCase.KIND_NOT_SET -> {}
    }
  }

  // === Parquet plumbing ===

  /**
   * Subclass of [ParquetReader.Builder] needed to reach the protected
   * `Builder(InputFile)` constructor. If [decryption] is non-null, install it
   * via the parquet-mr `withDecryption` builder API, enabling PME column
   * decryption.
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
   * Reads the parquet file's footer key-value metadata directly from the
   * thrift `FileMetaData` struct, bypassing parquet-mr's high-level
   * `ParquetFileReader`.
   *
   * Why bypass: under PME `PLAINTEXT_FOOTER` mode the footer body (schema,
   * key-value metadata) IS plaintext on disk, but per-column metadata is
   * still encrypted with the footer key. `ParquetFileReader.open` decrypts
   * ALL column metadata eagerly, which requires the footer key here —
   * defeating the point of reading the footer first to obtain key bootstrap
   * material.
   *
   * Solution: read the raw FileMetaData thrift struct via
   * `Util.readFileMetaData(in, skipRowGroups = true)`. Skipping row groups
   * means the encrypted per-column-metadata blobs are never touched; only
   * file-level fields, including the always-plaintext `key_value_metadata`,
   * are returned.
   *
   * Parquet footer trailer layout (last 8 bytes):
   * `[int32 footer length LE][4-byte magic]`. Magic is `"PAR1"` for
   * plaintext-footer files (including PME `PLAINTEXT_FOOTER`). `"PARE"`
   * indicates PME `ENCRYPTED_FOOTER`, which we explicitly REJECT: that mode
   * stores a `FileCryptoMetaData` thrift followed by the encrypted
   * `FileMetaData`, so the parse would hit ciphertext.
   */
  private fun readFooterKeyValueMetadata(path: Path): Map<String, String> {
    val inputFile = HadoopInputFile.fromPath(path, conf)
    val fileLen = inputFile.length
    require(fileLen >= MIN_PARQUET_FILE_SIZE) {
      "File too small to be parquet: $path (size=$fileLen)"
    }
    inputFile.newStream().use { stream ->
      // Read the 8-byte trailer: [footer length: int32 LE][magic: 4 bytes].
      val tail = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      stream.seek(fileLen - 8)
      stream.readFully(tail)
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
      require(magic == PARQUET_MAGIC_PLAINTEXT) { "Not a parquet file: $path (bad magic '$magic')" }
      require(footerLen in 1..(fileLen - 8)) {
        "Implausible parquet footer length $footerLen for file size $fileLen"
      }
      val footerStart = fileLen - 8 - footerLen
      val footerBuf = ByteBuffer.allocate(footerLen)
      stream.seek(footerStart)
      stream.readFully(footerBuf)
      footerBuf.flip()
      val footerBytes = ByteArray(footerLen).also { footerBuf.get(it) }
      val md = Util.readFileMetaData(ByteArrayInputStream(footerBytes), /* skipRowGroups = */ true)
      val kv = md.key_value_metadata ?: return emptyMap()
      return kv.associate { it.key to (it.value ?: "") }
    }
  }

  private companion object {
    private const val PARQUET_MAGIC_PLAINTEXT = "PAR1"
    private const val PARQUET_MAGIC_ENCRYPTED_FOOTER = "PARE"
    private const val MIN_PARQUET_FILE_SIZE = 12L // 4-byte header + 8-byte trailer minimum.

    /** Associated data for KMS DEK unwrapping (the EDP convention uses none). */
    private val EMPTY_AAD = ByteArray(0)

    private val NULL_VALUE: ParquetValue = ParquetValue.getDefaultInstance()

    /** Placeholder schema for empty writes (parquet rejects a zero-column schema). */
    private val EMPTY_SCHEMA: MessageType =
      MessageType("ParquetRow", Types.optional(PrimitiveTypeName.BINARY).named("placeholder"))

    private fun int32Value(v: Int) = ParquetValue.newBuilder().setInt32Value(v).build()

    private fun int64Value(v: Long) = ParquetValue.newBuilder().setInt64Value(v).build()

    private fun floatValue(v: Float) = ParquetValue.newBuilder().setFloatValue(v).build()

    private fun doubleValue(v: Double) = ParquetValue.newBuilder().setDoubleValue(v).build()

    private fun boolValue(v: Boolean) = ParquetValue.newBuilder().setBoolValue(v).build()

    private fun stringValue(v: String) = ParquetValue.newBuilder().setStringValue(v).build()

    private fun bytesValue(v: ByteString) = ParquetValue.newBuilder().setBytesValue(v).build()

    private fun timestampValue(instant: Instant): ParquetValue =
      ParquetValue.newBuilder()
        .setTimestampValue(
          Timestamp.newBuilder().setSeconds(instant.epochSecond).setNanos(instant.nano).build()
        )
        .build()

    private fun dateValue(date: LocalDate): ParquetValue =
      ParquetValue.newBuilder()
        .setDateValue(
          Date.newBuilder()
            .setYear(date.year)
            .setMonth(date.monthValue)
            .setDay(date.dayOfMonth)
            .build()
        )
        .build()

    /**
     * Maps a native column value (from [ColumnDecoder.extract]) to a
     * [ParquetValue] for the [read] proto codec. `null` -> unset (NULL).
     */
    private fun valueToProto(value: Any?): ParquetValue =
      when (value) {
        null -> NULL_VALUE
        is Int -> int32Value(value)
        is Long -> int64Value(value)
        is Float -> floatValue(value)
        is Double -> doubleValue(value)
        is Boolean -> boolValue(value)
        is String -> stringValue(value)
        is ByteString -> bytesValue(value)
        is Instant -> timestampValue(value)
        is LocalDate -> dateValue(value)
        else ->
          throw IllegalStateException(
            "Unsupported native value type ${value.javaClass.name} for ParquetValue conversion"
          )
      }

    private fun instantOf(raw: Long, unit: LogicalTypeAnnotation.TimeUnit): Instant =
      when (unit) {
        LogicalTypeAnnotation.TimeUnit.MILLIS -> Instant.ofEpochMilli(raw)
        LogicalTypeAnnotation.TimeUnit.MICROS ->
          Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000L), Math.floorMod(raw, 1_000_000L) * 1_000L)
        LogicalTypeAnnotation.TimeUnit.NANOS ->
          Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000_000L), Math.floorMod(raw, 1_000_000_000L))
      }

    private fun instantToMicros(ts: Timestamp): Long =
      Math.addExact(Math.multiplyExact(ts.seconds, 1_000_000L), (ts.nanos / 1_000).toLong())

    private fun dateToEpochDay(date: Date): Long =
      LocalDate.of(date.year, date.month, date.day).toEpochDay()
  }
}

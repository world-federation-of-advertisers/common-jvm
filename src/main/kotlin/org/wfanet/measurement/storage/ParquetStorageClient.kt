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
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.jetbrains.annotations.BlockingExecutor

/**
 * A wrapper for [StorageClient] that exposes parquet-aware reads with
 * **typed** per-row projections.
 *
 * Composes naturally with other [StorageClient] wrappers — e.g. envelope
 * encryption around the underlying blob bytes:
 * ```
 * GcsStorageClient                       (raw bytes)
 *   wrapped by StreamingAeadStorageClient   (decrypted bytes)
 *     wrapped by ParquetStorageClient       (parquet rows projected to proto + footer metadata)
 * ```
 *
 * Each [Projection] declares (a) a target proto [Descriptors.Descriptor],
 * and (b) a `fieldMapping` from target-field path → source parquet column
 * name. For each parquet row, the wrapper produces one [DynamicMessage]
 * per configured [Projection], with proto fields populated by reading the
 * corresponding parquet columns.
 *
 * Multiple projections per construction support the common case of one
 * source row populating several target schemas (e.g. a labeler-input proto
 * and a market event template) in a single file pass.
 *
 * Column pushdown: only parquet columns referenced by a [Projection] are
 * decoded. The wrapper installs a [ReadSupport] that projects parquet's
 * read schema to the requested-column subset; unmapped columns are not
 * decoded. For wide EDP rows (dozens of columns mapped to a handful)
 * this is a substantial CPU/IO saving over reading the full row.
 *
 * Hadoop runtime classes ARE required at runtime: parquet-mr 1.14.x
 * instantiates a `Hadoop Configuration` from inside `ParquetReader.Builder`
 * constructors and `ParquetFileReader.open`, and `ParquetReadOptions.Builder`
 * also references `org.apache.hadoop.mapreduce.lib.input.FileInputFormat`.
 * The `hadoop-common` and `hadoop-mapreduce-client-core` dependencies are
 * therefore real runtime requirements.
 *
 * Materialisation: parquet's footer-at-end layout requires a
 * [SeekableInputStream]. The wrapper materialises the underlying blob into
 * a `byte[]` once per [ParquetBlob] instance and caches it across the three
 * read methods, so callers that need both row projections and footer
 * metadata pay one underlying read. The internal buffer of the materialise
 * sink is reused without a defensive `toByteArray()` copy.
 *
 * Hot-path note: parquet decode and the materialisation `collect` are
 * blocking. All three read methods run their blocking sections under
 * [parquetContext] (default [Dispatchers.IO]) via `flowOn` / `withContext`.
 *
 * Writes pass through to the underlying [StorageClient] unchanged — this
 * wrapper does not encode parquet on the write side. Producers should
 * already supply parquet-encoded bytes.
 *
 * Supported projection types:
 *  - Top-level and arbitrarily-nested message paths in the proto target
 *    (dot notation, e.g. `"event_id.id"`).
 *  - Proto field types `INT`, `LONG`, `FLOAT`, `DOUBLE`, `BOOLEAN`,
 *    `STRING`, `BYTE_STRING`, `ENUM` (enum is matched against the parquet
 *    column's primitive type: parquet `INT32` → enum number,
 *    parquet `BINARY/UTF8` → enum name).
 *  - Direct message-typed leaves and repeated leaves are NOT supported —
 *    rejected at construction (for repeated) or at read (for direct
 *    message-typed leaves).
 *
 * Validation phasing — important to understand when each kind of
 * misconfiguration throws:
 *  - **Construction (`IllegalArgumentException`)**: projection name
 *    uniqueness; target field paths must exist in the target proto
 *    descriptor; intermediate path segments must be message-typed; leaf
 *    fields must be non-repeated.
 *  - **First row read per blob (`IllegalStateException` inside the flow
 *    at `collect` time)**: source parquet column existence; per-row
 *    repetition (a REPEATED parquet column with `count > 1` mapped to a
 *    non-repeated proto target is rejected).
 *  - **Each row at decode (`IllegalStateException`)**: enum integer/name
 *    not present in the target enum type.
 *
 * Limitations / caveats:
 *  - Source parquet column paths are FLAT (top-level only). Nested
 *    parquet `Group` source columns are not supported.
 *  - Proto `uint32`/`uint64` targets read as signed `Int`/`Long`; values
 *    above `Int.MAX_VALUE`/`Long.MAX_VALUE` silently wrap (JVM has no
 *    unsigned ints — a proto-on-JVM quirk).
 *  - [listBlobs] returns the underlying client's `StorageClient.Blob`,
 *    NOT a [ParquetBlob]. Use [getBlob] to obtain a parquet-aware blob.
 *    (Matches the convention used by `MesosRecordIoStorageClient`.)
 *  - [StorageClient.Blob.size] reports the underlying blob's size — when
 *    wrapped by a decrypting client this is the ciphertext size, NOT the
 *    materialised parquet size.
 *  - First concurrent call to any read method that triggers materialise
 *    will serialise the others through the materialise mutex. After the
 *    first call resolves, subsequent calls hit the volatile fast path.
 *
 * Known follow-up (not addressed here): the full in-memory materialise caps
 * supported file size and pressures heap. A future range-read-backed
 * [SeekableInputStream] would make reads genuinely streaming, but requires
 * adding a range-read primitive to the [StorageClient] interface.
 *
 * @param storageClient underlying client for accessing blob/object storage.
 * @param projections projections to populate per parquet row.
 * @param parquetContext blocking context for parquet decode + materialise.
 */
class ParquetStorageClient(
  private val storageClient: StorageClient,
  private val projections: List<Projection>,
  private val parquetContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient {

  /**
   * Declares one typed projection from parquet rows to a target proto.
   *
   * @param name caller-chosen identifier — used as the map key in
   *   [ParquetBlob.readProjectedMessages] and as the lookup argument in
   *   [ParquetBlob.readMessages]. Must be unique across the projections
   *   passed to a single [ParquetStorageClient].
   * @param descriptor target proto descriptor whose fields are populated.
   * @param fieldMapping `key = target field path within descriptor (dot
   *   notation for nesting); value = source parquet column name`.
   */
  data class Projection(
    val name: String,
    val descriptor: Descriptors.Descriptor,
    val fieldMapping: Map<String, String>,
  )

  /**
   * A [StorageClient.Blob] that exposes parquet-aware reads driven by the
   * [Projection]s supplied to its [ParquetStorageClient].
   */
  interface ParquetBlob : StorageClient.Blob {
    /**
     * Cold flow of typed projected messages. One emission per parquet row;
     * each emission is a `Map<projectionName, DynamicMessage>` covering
     * every configured [Projection].
     */
    fun readProjectedMessages(): Flow<Map<String, DynamicMessage>>

    /**
     * Cold flow of one projection. Equivalent to
     * `readProjectedMessages().map { it.getValue(projectionName) }` but
     * avoids constructing the per-row map AND restricts parquet column
     * pushdown to only this projection's source columns. As a result,
     * `readMessages("a")` will not fail because of a misconfiguration in
     * an unrelated projection `"b"`.
     */
    fun readMessages(projectionName: String): Flow<DynamicMessage>

    /** Parquet file's footer key/value metadata. */
    suspend fun readKeyValueMetadata(): Map<String, String>
  }

  // Validate uniqueness BEFORE resolver construction so duplicate-name
  // errors win over per-projection path errors.
  init {
    require(projections.distinctBy { it.name }.size == projections.size) {
      "Projection names must be unique; got duplicates in ${projections.map { it.name }}"
    }
  }

  // Pre-compile target field paths at construction so the per-row
  // projection cost is just navigation + value reads, not string parsing.
  private val resolvers: List<ProjectionResolver> = projections.map(::ProjectionResolver)

  private val resolversByName: Map<String, ProjectionResolver> = resolvers.associateBy { it.name }

  // === StorageClient — implemented explicitly (no `by` delegation) to ===
  // === match the convention used by MesosRecordIoStorageClient.       ===

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return storageClient.writeBlob(blobKey, content)
  }

  override suspend fun getBlob(blobKey: String): ParquetBlob? {
    val raw = storageClient.getBlob(blobKey) ?: return null
    return ParquetBlobImpl(raw)
  }

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return storageClient.listBlobs(prefix)
  }

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

    // Cached materialised view, shared across readProjectedMessages /
    // readMessages / readKeyValueMetadata — pay the underlying read once.
    @Volatile private var cached: Materialised? = null
    private val cacheMutex = Mutex()

    // Per-resolver-name lazy bind cache. Each entry materialises on first
    // use of THAT projection only — readMessages("a") never forces a
    // bind() for unrelated projection "b".
    private val boundCache = ConcurrentHashMap<String, BoundResolver>()

    private suspend fun materialised(): Materialised {
      cached?.let { return it }
      return cacheMutex.withLock {
        cached?.let { return it }
        val sink = GrowableByteBuffer()
        delegate.read().collect { chunk -> chunk.writeTo(sink) }
        Materialised(sink.internalBuffer(), sink.length()).also { cached = it }
      }
    }

    private fun bindOne(name: String, schema: GroupType): BoundResolver {
      return boundCache.computeIfAbsent(name) {
        val resolver =
          resolversByName[name]
            ?: error("No Projection named '$name'; available: ${resolversByName.keys}")
        resolver.bind(schema)
      }
    }

    override fun readProjectedMessages(): Flow<Map<String, DynamicMessage>> =
      flow {
          require(resolvers.isNotEmpty()) {
            "readProjectedMessages requires at least one Projection at construction"
          }
          // Union of source columns across all projections for pushdown.
          val requested = resolvers.flatMap { it.sourceColumns }.toSet()
          val inputFile = materialised().toInputFile()
          GroupParquetReaderBuilder(inputFile, requested).build().use { reader ->
            val first = reader.read() ?: return@use
            val bound = resolvers.map { bindOne(it.name, first.type) }
            emit(bound.associate { it.name to it.project(first) })
            var group: Group? = reader.read()
            while (group != null) {
              val current = group
              emit(bound.associate { it.name to it.project(current) })
              group = reader.read()
            }
          }
        }
        .flowOn(parquetContext)

    override fun readMessages(projectionName: String): Flow<DynamicMessage> =
      flow {
          val resolver =
            resolversByName[projectionName]
              ?: throw IllegalArgumentException(
                "No Projection named '$projectionName'; available: ${resolversByName.keys}"
              )
          // Pushdown only this projection's source columns.
          val inputFile = materialised().toInputFile()
          GroupParquetReaderBuilder(inputFile, resolver.sourceColumns).build().use { reader ->
            val first = reader.read() ?: return@use
            val bound = bindOne(projectionName, first.type)
            emit(bound.project(first))
            var group: Group? = reader.read()
            while (group != null) {
              emit(bound.project(group))
              group = reader.read()
            }
          }
        }
        .flowOn(parquetContext)

    override suspend fun readKeyValueMetadata(): Map<String, String> =
      withContext(parquetContext) {
        val inputFile = materialised().toInputFile()
        ParquetFileReader.open(inputFile).use { reader ->
          reader.fileMetaData.keyValueMetaData.toMap()
        }
      }
  }

  // === Materialisation primitive ===

  /**
   * Zero-copy holder for the decrypted blob bytes. [buffer] is the
   * underlying `ByteArrayOutputStream` internal array (may be larger than
   * [length] — only `[0, length)` is valid).
   */
  private class Materialised(val buffer: ByteArray, val length: Int) {
    fun toInputFile(): InputFile = ByteArrayInputFile(buffer, length)
  }

  /**
   * [ByteArrayOutputStream] subclass that exposes its internal buffer
   * without the `toByteArray()` defensive copy — avoids doubling peak heap
   * during materialise of large blobs.
   */
  private class GrowableByteBuffer : ByteArrayOutputStream() {
    fun internalBuffer(): ByteArray = buf

    fun length(): Int = count
  }

  // === Projector: compiles a Projection into per-target metadata. ===

  /**
   * Compiles a [Projection] into resolved target paths. Field-by-field
   * binding against the parquet schema (validation + repetition + parquet
   * type) is deferred to [bind], which is invoked once per blob.
   */
  private class ProjectionResolver(projection: Projection) {
    val name: String = projection.name
    private val descriptor: Descriptors.Descriptor = projection.descriptor

    /** Resolved target path; bound to a parquet column at [bind] time. */
    data class Resolved(
      val path: List<Descriptors.FieldDescriptor>,
      val sourceColumn: String,
    )

    val resolvedMappings: List<Resolved> =
      projection.fieldMapping.map { (targetPath, sourceColumn) ->
        require(targetPath.isNotBlank()) {
          "Empty target field path in projection '${projection.name}'"
        }
        require(sourceColumn.isNotBlank()) {
          "Empty source column for target '$targetPath' in projection '${projection.name}'"
        }
        val parts = targetPath.split('.')
        val chain = mutableListOf<Descriptors.FieldDescriptor>()
        var currentDescriptor: Descriptors.Descriptor = projection.descriptor
        parts.forEachIndexed { idx, partName ->
          val field =
            currentDescriptor.findFieldByName(partName)
              ?: throw IllegalArgumentException(
                "Projection '${projection.name}' target path '$targetPath' references " +
                  "non-existent field '$partName' in ${currentDescriptor.fullName}"
              )
          chain.add(field)
          if (idx < parts.size - 1) {
            require(field.javaType == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
              "Projection '${projection.name}' target path '$targetPath' navigates through " +
                "non-message field '$partName' (javaType=${field.javaType})"
            }
            currentDescriptor = field.messageType
          }
        }
        val leaf = chain.last()
        require(!leaf.isRepeated) {
          "Projection '${projection.name}' target '$targetPath' is a repeated field — not supported"
        }
        Resolved(chain.toList(), sourceColumn)
      }

    /** The set of parquet column names this projection reads. */
    val sourceColumns: Set<String> = resolvedMappings.map { it.sourceColumn }.toSet()

    init {
      require(name.isNotBlank()) { "Projection.name must be non-blank" }
    }

    /** Validates against a parquet schema and returns a per-row projector. */
    fun bind(schema: GroupType): BoundResolver {
      val bound =
        resolvedMappings.map { resolved ->
          if (!schema.containsField(resolved.sourceColumn)) {
            error(
              "Projection '$name' references parquet column '${resolved.sourceColumn}' that " +
                "is not in the row schema. Available: ${schema.fields.map { it.name }}"
            )
          }
          val sourceField = schema.getType(resolved.sourceColumn)
          val primitive: PrimitiveType.PrimitiveTypeName? =
            if (sourceField.isPrimitive) sourceField.asPrimitiveType().primitiveTypeName else null
          BoundResolver.Bound(resolved, sourceField.repetition, primitive)
        }
      return BoundResolver(name, descriptor, bound)
    }
  }

  /**
   * Schema-bound projector for one [Projection] against one blob's parquet
   * schema. Per-row `project(group)` does only column reads + setField
   * calls — no string lookups in the parquet schema (paid once at [bind]).
   */
  private class BoundResolver(
    val name: String,
    private val descriptor: Descriptors.Descriptor,
    private val mappings: List<Bound>,
  ) {
    /** A resolved target path bound to a parquet column with cached metadata. */
    data class Bound(
      val resolved: ProjectionResolver.Resolved,
      val repetition: Type.Repetition,
      val primitive: PrimitiveType.PrimitiveTypeName?,
    )

    /** Projects one parquet row [Group] into a [DynamicMessage] of [descriptor]. */
    fun project(group: Group): DynamicMessage {
      val builder = DynamicMessage.newBuilder(descriptor)
      mappings.forEach { bound ->
        val value = readValue(group, bound) ?: return@forEach
        setNested(builder, bound.resolved.path, value)
      }
      return builder.build()
    }

    private fun readValue(group: Group, bound: Bound): Any? {
      val column = bound.resolved.sourceColumn
      // Repetition-aware fast path: REQUIRED is guaranteed count==1.
      // OPTIONAL needs the empty check. REPEATED additionally rejects
      // count>1 since the target proto field is non-repeated (validated
      // at construction).
      when (bound.repetition) {
        Type.Repetition.REQUIRED -> {
          /* count is always 1 — skip the per-row getFieldRepetitionCount call */
        }
        Type.Repetition.OPTIONAL -> {
          if (group.getFieldRepetitionCount(column) == 0) return null
        }
        Type.Repetition.REPEATED -> {
          val count = group.getFieldRepetitionCount(column)
          if (count == 0) return null
          if (count > 1) {
            error(
              "Projection '$name' source column '$column' has $count values in this row; " +
                "mapping a REPEATED parquet column to a non-repeated proto target is " +
                "not supported."
            )
          }
        }
      }
      val targetField = bound.resolved.path.last()
      // getFieldBuilder for nested paths is called per row inside setNested;
      // proto's DynamicMessage.Builder makes this cheap.
      return when (targetField.javaType) {
        Descriptors.FieldDescriptor.JavaType.INT -> group.getInteger(column, 0)
        Descriptors.FieldDescriptor.JavaType.LONG -> group.getLong(column, 0)
        Descriptors.FieldDescriptor.JavaType.FLOAT -> group.getFloat(column, 0)
        Descriptors.FieldDescriptor.JavaType.DOUBLE -> group.getDouble(column, 0)
        Descriptors.FieldDescriptor.JavaType.BOOLEAN -> group.getBoolean(column, 0)
        Descriptors.FieldDescriptor.JavaType.STRING -> group.getBinary(column, 0).toStringUsingUTF8()
        Descriptors.FieldDescriptor.JavaType.BYTE_STRING ->
          // toByteBuffer + ByteString.copyFrom avoids the second copy
          // Binary.getBytes -> ByteString.copyFrom(byte[]) would cost.
          ByteString.copyFrom(group.getBinary(column, 0).toByteBuffer())
        Descriptors.FieldDescriptor.JavaType.ENUM -> {
          val enumType = targetField.enumType
          when (bound.primitive) {
            PrimitiveType.PrimitiveTypeName.INT32 -> {
              val number = group.getInteger(column, 0)
              enumType.findValueByNumber(number)
                ?: error(
                  "Projection '$name' enum target '${targetField.fullName}' has no value with " +
                    "number=$number (from parquet column '$column'). Known values: " +
                    enumType.values.map { it.number to it.name }
                )
            }
            PrimitiveType.PrimitiveTypeName.BINARY -> {
              val str = group.getBinary(column, 0).toStringUsingUTF8()
              enumType.findValueByName(str)
                ?: error(
                  "Projection '$name' enum target '${targetField.fullName}' has no value with " +
                    "name='$str' (from parquet column '$column'). Known values: " +
                    enumType.values.map { it.name }
                )
            }
            else ->
              error(
                "Projection '$name' enum target '${targetField.fullName}' must be sourced from " +
                  "a parquet INT32 (enum number) or BINARY/UTF8 (enum name) column. " +
                  "Got parquet primitive=${bound.primitive} on column '$column'."
              )
          }
        }
        Descriptors.FieldDescriptor.JavaType.MESSAGE ->
          error(
            "Projection '$name' leaf field '${targetField.fullName}' has unsupported " +
              "javaType=MESSAGE. Direct message-typed leaves are not supported; use " +
              "dot-notation paths into nested-message fields instead."
          )
      }
    }

    private fun setNested(
      builder: Message.Builder,
      path: List<Descriptors.FieldDescriptor>,
      value: Any,
    ) {
      if (path.size == 1) {
        builder.setField(path[0], value)
        return
      }
      val parent = path[0]
      // getFieldBuilder returns the same Builder instance for the same
      // message field on repeated calls within one row — multiple mappings
      // into the same nested message correctly share one parent builder.
      val nestedBuilder = builder.getFieldBuilder(parent)
      setNested(nestedBuilder, path.drop(1), value)
    }
  }

  // === Plumbing: parquet-mr Builder access + projected ReadSupport ===

  /**
   * Subclass of [ParquetReader.Builder] needed to reach the protected
   * `Builder(InputFile)` constructor — parquet-mr's public `builder()`
   * static factory only accepts Hadoop `Path`. Installs a
   * [ProjectedGroupReadSupport] so parquet only decodes the requested
   * column subset (column pushdown).
   */
  private class GroupParquetReaderBuilder(file: InputFile, private val requestedColumns: Set<String>) :
    ParquetReader.Builder<Group>(file) {
    override fun getReadSupport(): ReadSupport<Group> =
      ProjectedGroupReadSupport(requestedColumns)
  }

  /**
   * [GroupReadSupport] that asks parquet to decode only the columns whose
   * name appears in [requestedColumns]. Columns referenced by the
   * projection but not in the file schema are dropped from the projected
   * schema; the per-blob bind step will surface a clear error for them.
   */
  private class ProjectedGroupReadSupport(private val requestedColumns: Set<String>) :
    GroupReadSupport() {
    override fun init(context: InitContext): ReadContext {
      if (requestedColumns.isEmpty()) {
        return super.init(context)
      }
      val fullSchema = context.fileSchema
      val projectedFields: List<Type> =
        fullSchema.fields.filter { it.name in requestedColumns }
      if (projectedFields.isEmpty()) {
        // No intersection between requested and file schema. Fall back to
        // the full schema so the per-blob bind step can produce a clear
        // "column not in schema" error rather than parquet's internal one.
        return super.init(context)
      }
      val projected = MessageType(fullSchema.name, projectedFields)
      return ReadContext(projected)
    }
  }

  /**
   * Minimal in-memory [InputFile] backed by a `byte[]`. Parquet needs a
   * seekable input because the footer lives at the end of the file. The
   * underlying buffer may be larger than [length] (e.g. when reusing a
   * `ByteArrayOutputStream`'s internal buffer); only `[0, length)` is read.
   */
  private class ByteArrayInputFile(private val data: ByteArray, private val length: Int) :
    InputFile {
    init {
      require(length in 0..data.size) { "length($length) out of [0, ${data.size}]" }
    }

    override fun getLength(): Long = length.toLong()

    override fun newStream(): SeekableInputStream = ByteArraySeekableInputStream(data, length)
  }

  private class ByteArraySeekableInputStream(private val data: ByteArray, private val limit: Int) :
    SeekableInputStream() {
    private var pos: Int = 0

    override fun getPos(): Long = pos.toLong()

    override fun seek(newPos: Long) {
      require(newPos in 0..limit.toLong()) { "seek($newPos) out of range [0, $limit]" }
      pos = newPos.toInt()
    }

    override fun read(): Int = if (pos < limit) data[pos++].toInt() and 0xFF else -1

    override fun read(buf: ByteArray, off: Int, len: Int): Int {
      // InputStream contract: zero-length request returns 0, not -1, even at EOF.
      if (len == 0) return 0
      if (pos >= limit) return -1
      val available = minOf(len, limit - pos)
      System.arraycopy(data, pos, buf, off, available)
      pos += available
      return available
    }

    override fun readFully(buf: ByteArray) = readFully(buf, 0, buf.size)

    override fun readFully(buf: ByteArray, off: Int, len: Int) {
      if (pos + len > limit) {
        throw EOFException("readFully past end: pos=$pos, len=$len, limit=$limit")
      }
      System.arraycopy(data, pos, buf, off, len)
      pos += len
    }

    override fun read(buf: ByteBuffer): Int {
      // InputStream contract: zero-length request returns 0, not -1, even at EOF.
      if (buf.remaining() == 0) return 0
      if (pos >= limit) return -1
      val available = minOf(buf.remaining(), limit - pos)
      buf.put(data, pos, available)
      pos += available
      return available
    }

    override fun readFully(buf: ByteBuffer) {
      val needed = buf.remaining()
      if (pos + needed > limit) {
        throw EOFException(
          "readFully(ByteBuffer) past end: pos=$pos, need=$needed, limit=$limit"
        )
      }
      buf.put(data, pos, needed)
      pos += needed
    }

    @Throws(IOException::class) override fun close() = Unit
  }
}

// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.riegeli

import com.google.highwayhash.HighwayHash
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.LinkedList
import java.util.logging.Logger
import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.commons.compress.utils.CountingInputStream
import org.wfanet.measurement.common.getVarInt64
import org.wfanet.measurement.common.toLong
import org.wfanet.measurement.common.withTrailingPadding

/**
 * Native kotlin implementation of a Riegeli decompressor.
 *
 * Follows the Riegeli/Records standard as specified here:
 * https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md
 */
object Riegeli {

  /**
   * Constant representing the fixed-size of a block within a Riegeli compressed file.
   */
  private const val BLOCK_SIZE = 1 shl 16

  /**
   * builds a HighwayHash object with the key
   * 'Riegeli/', 'records\n', 'Riegeli/', 'records\n'
   *
   * @return A HighwayHash object with specified key.
   */
  private fun buildHighwayHash(): HighwayHash {
    return HighwayHash(0x2f696c6567656952, 0x0a7364726f636572, 0x2f696c6567656952, 0x0a7364726f636572)
  }

  /**
   * Reads and decompresses a Riegeli compressed input stream with records.
   *
   * @param incomingInputStream An input stream which contains data which should be decompressed using Riegeli.
   * @return A sequence where each element is a ByteString containing the bytes of a record.
   */
  fun readCompressedInputStreamWithRecords(incomingInputStream: InputStream): Sequence<ByteString> {
    return sequence<ByteString> {

      val inputStream = CountingInputStream(incomingInputStream)

      // Riegeli files start with a block header. Read and discard this block header.
      BlockHeader.readFrom(inputStream)

      while (true) {
        val chunk = Chunk.readFrom(inputStream) ?: break

        //Chunk type is simple chunk with records (0x72)
        if (chunk.chunkType == 0x72.toByte()) {
          logger.finer { "RECORD CHUNK -- Chunk type: ${chunk.chunkType}" }

          yieldAll(chunk.getRecords())

        } else {
          //Ignored/unsupported chunk types:
          //0x73 - File Signature - Present at the beginning of the file, encodes no records, is ignored
          //0x6d - File Metadata - provides information describing the records, not necessary to read, is ignored
          //0x70 - Padding - encodes no records and only occupies file space, is ignored
          //0x74 - Transposed ChuFnk with Records - no documentation provided for the format of this chunk, unimplementable

          logger.finer { "NON RECORD CHUNK -- Chunk type: ${chunk.chunkType}" }
        }
      }
    }
  }

  /**
   * Reads and decompresses a Riegeli compressed file with records.
   *
   * @param file The file which should be decompressed using Riegeli.
   * @return A sequence where each element is a ByteString containing the bytes of a record.
   */
  fun readCompressedFileWithRecords(file: File): List<ByteString> {
    file.inputStream().use { inputStream ->
      return readCompressedInputStreamWithRecords(inputStream).toList()
    }
  }

  private val logger = Logger.getLogger(this::class.java.name)

  /**
   * Represents the contents of a chunk within a Riegeli compressed file.
   *
   * The full specification of a chunk is provided here:
   *   https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md#chunk
   *
   * Chunks contain a chunk header (40 bytes) followed by data.
   *
   * Chunks are interrupted by block headers at every multiple of the block size (which is 64 KiB).
   *
   * @param headerHash      BteString containing a HighwayHash of the other chunk header elements
   *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
   * @param dataSize        ByteString containing the size of the chunk data, excluding intervening block headers.
   *                        The ByteString should be storing a 64-bit, little-endian unsigned integer.
   * @param dataHash        ByteString containing a HighwayHash of the data within the chunk.
   * @param chunkType       Byte containing a single byte which represents the chunk type.
   *                          - If chunkType is 0x6d, the chunk is a file metadata chunk.
   *                          - If chunkType is 0x70, the chunk is a padding chunk.
   *                          - If chunkType is 0x72, the chunk is a simple chunk with records.
   *                          - if chunkType is 0x74, the chunk is a transposed chunk with records.
   * @param numRecords      ByteString representing the number of records inside the chunk. The ByteString should be
   *                        storing a 56-bit, little-endian unsigned integer.
   * @param decodedDataSize ByteString representing the size of the chunk's data after it has been decoded.
   *                        The ByteString should be storing a 64-bit, little-endian unsigned integer.
   * @param data            ByteBuffer with the chunk's compressed data. The buffer's size is represented by dataSize.
   * @throws IllegalArgumentException   if the chunk's header does not hash properly, meaning that the chunk header
   *                                    is corrupted.
   * @throws IllegalArgumentException   if the hash of the chunk's data does not match dataHash within the chunk header,
   *                                    meaning the chunk's data is corrupted.
   */
  private class Chunk private constructor(val headerHash: ByteString,
                                          val dataSize: ByteString,
                                          val dataHash: ByteString,
                                          val chunkType: Byte,
                                          val numRecords: ByteString,
                                          val decodedDataSize: ByteString,
                                          val data: ByteBuffer) {

    init {
      require(isValidHeader(headerHash, dataSize, dataHash, chunkType, numRecords, decodedDataSize)) {
        "Riegeli: Chunk header is invalid"
      }

      require(isValidData(dataSize, dataHash, data)) {
        "Riegeli: Chunk data is invalid"
      }
    }

    companion object {

      /**
       * Constructs and returns a new Chunk object from an input stream.
       *
       * Validates the chunk header and data against their respective hashes.
       *
       * Identifies and skips intervening block headers.
       *
       * @param inputStream Counting input stream from which the chunk should be read. The input stream's count must
       *                    have started at the beginning of a file.
       * @return A new chunk object as read from the inputStream.
       */
      fun readFrom(inputStream: CountingInputStream): Chunk? {

        val headerHashArray = ByteArray(8)
        val dataSizeArray = ByteArray(8)
        val dataHashArray = ByteArray(8)
        val numRecordsArray = ByteArray(7)
        val decodedDataSizeArray = ByteArray(8)

        if (inputStream.read(headerHashArray) == -1) {
          //There is no more to read
          return null
        }

        inputStream.read(dataSizeArray)
        inputStream.read(dataHashArray)

        val chunkType = inputStream.read().toByte()

        inputStream.read(numRecordsArray)
        inputStream.read(decodedDataSizeArray)

        val headerHash = headerHashArray.toByteString()
        val dataSize = dataSizeArray.toByteString()
        val dataHash = dataHashArray.toByteString()
        val numRecords = numRecordsArray.toByteString()
        val decodedDataSize = decodedDataSizeArray.toByteString()

        val dataStream = ByteArrayOutputStream()
        val dataSizeInt = dataSize.toLong(ByteOrder.LITTLE_ENDIAN).toInt()

        repeat(dataSizeInt) {

          if (inputStream.bytesRead % BLOCK_SIZE == 0L) {
            BlockHeader.readFrom(inputStream)
          }

          dataStream.write(inputStream.read())
        }

        val data = ByteBuffer.wrap(dataStream.toByteArray())

        return Chunk(headerHash, dataSize, dataHash, chunkType, numRecords, decodedDataSize, data)
      }

      /**
       * Helper function for the chunk builder function.
       * Compares the headerHash element of the chunk's header to a highway hash of the chunk's other header elements.
       *
       * @param headerHash      64-bit ByteString containing a HighwayHash of the other chunk header elements
       *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
       * @param dataSize        ByteString representing the size of the chunk data, excluding intervening block headers.
       *                        The ByteString should be storing a 64-bit, little-endian unsigned integer.
       * @param dataHash        ByteString containing a HighwayHash of the data within the chunk.
       * @param chunkType       Byte containing a single byte which represents the chunk type.
       * @param numRecords      ByteString representing the number of records inside the chunk. The ByteString is
       *                        storing a 56-bit, little-endian unsigned integer.
       * @param decodedDataSize ByteString representing the size of the chunk's data after it has been decoded.
       *                        The ByteString should be storing a 64-bit, little-endian unsigned integer.
       * @return True if the chunk's header is valid, false if it is not.
       */
      private fun isValidHeader(headerHash: ByteString,
                                dataSize: ByteString,
                                dataHash: ByteString,
                                chunkType: Byte,
                                numRecords: ByteString,
                                decodedDataSize: ByteString): Boolean {

        val highwayHash = buildHighwayHash()

        val headerData = dataSize + dataHash + chunkType + numRecords + decodedDataSize

        highwayHash.updatePacket(headerData.toByteArray(), 0)

        val hashedDataLong = highwayHash.finalize64()

        val headerHashLong = headerHash.toLong(ByteOrder.LITTLE_ENDIAN)

        return hashedDataLong == headerHashLong
      }

      /**
       * Helper function for the chunk builder function.
       * Compares the dataHash element of the chunk's header to a highway hash of the chunk's data.
       *
       * @param dataSize  ByteString representing the size of the chunk data, excluding intervening block headers.
       *                  The ByteString should be storing a 64-bit, little-endian unsigned integer.
       * @param data      ByteBuffer with the chunk's compressed data. The buffer's size is represented by dataSize.
       * @return True if the chunk's header is valid, false if it is not.
       */
      private fun isValidData(dataSize: ByteString, dataHash: ByteString, data: ByteBuffer): Boolean {
        val highwayHash = buildHighwayHash()

        var position = 0
        val dataSizeInt = dataSize.toLong(ByteOrder.LITTLE_ENDIAN).toInt()

        while (dataSizeInt - position >= 32) {
          highwayHash.updatePacket(data.array(), position)
          position += 32
        }

        if (dataSizeInt - position > 0) {
          highwayHash.updateRemainder(data.array(), position, dataSizeInt - position)
        }

        val dataHashLong = dataHash.toLong(ByteOrder.LITTLE_ENDIAN)
        val hashedDataLong = highwayHash.finalize64()

        return dataHashLong == hashedDataLong
      }
    }

    /**
     * Reads a compressed buffer from a chunk with records.
     *
     * Compressed buffers, if compressionType is not 0, are prefixed with a varint containing their decompressed size.
     *
     * @param dataBuffer      The ByteBuffer from which the compressed buffer should be read.
     * @param compressedSize  The size of the buffer, the number of bytes that should be read from the input stream.
     * @param compressionType A Byte representing the type of compression used in the compressed buffer.
     *                          - 0x00: none
     *                          - 0x62: Brotli
     *                          - 0x7a: Zstd
     *                          - 0x73: Snappy
     * @return A ByteBuffer containing the decompressed data from the buffer.
     */
    private fun readCompressedBuffer(dataBuffer: ByteBuffer, compressedSize: Int, compressionType: Byte): ByteBuffer {
      val startingPoint = dataBuffer.position()

      //If compression type is 0, there is not a varint at the beginning of the buffer so do not read it.
      val decompressedSize = if (compressionType == 0x00.toByte()) -1 else dataBuffer.getVarInt64().toInt()

      val sizeOfVarInt = (dataBuffer.position() - startingPoint)

      val compressedBuffer = ByteBuffer.allocate(compressedSize - sizeOfVarInt)

      while (compressedBuffer.hasRemaining()) {
        compressedBuffer.put(dataBuffer.get())
      }

      // If compression type is 0, the output stream is already decompressed
      if (compressionType == 0x00.toByte()) {
        return compressedBuffer
      }

      val tempInputStream = ByteArrayInputStream(compressedBuffer.array())

      val compressedInputStream: CompressorInputStream = when (compressionType) {
        0x62.toByte() -> BrotliCompressorInputStream(tempInputStream)
        0x7a.toByte() -> ZstdCompressorInputStream(tempInputStream)
        0x73.toByte() -> SnappyCompressorInputStream(tempInputStream)
        else -> throw Exception("Invalid Compression Type for Compressed Buffer")
      }

      val uncompressedBytes = ByteArray(decompressedSize)

      compressedInputStream.read(uncompressedBytes)

      return ByteBuffer.wrap(uncompressedBytes)
    }

    /**
     * The full specification for a chunk with records can be found here:
     *  https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md#simple-chunk-with-records
     *
     * A chunk with record's data contains the following:
     *  - compressionType:      A single byte representing which type of compression was used within the chunk.
     *  - compressedSizesSize:  A varint representing the size of compressedSizes
     *  - compressedSizes:      A compressed buffer of size compressedSizesSize. Contains numRecords varints, the size
     *                          of each record.
     *  - compressedValues:     A compressed buffer with the record values. After decompression,
     *                          contains decodedDataSize bytes.
     *
     * @return  A list where each element is a ByteString representing a decompressed record. If there are no records
     *          within the chunk, an empty list is returned.
     */
    fun getRecords(): List<ByteString> {

      //If the chunk is not a chunk with records, return an empty list
      if (this.chunkType != 0x72.toByte()) {
        return emptyList()
      }

      val dataBuffer = this.data.duplicate().rewind()

      val startingPoint = dataBuffer.position()

      val compressionType = dataBuffer.get()

      val compressedSizesSizeInt = dataBuffer.getVarInt64().toInt()

      val numRecords = this.numRecords.withTrailingPadding(8).toLong(ByteOrder.LITTLE_ENDIAN).toInt()

      val sizesByteBuffer = readCompressedBuffer(dataBuffer, compressedSizesSizeInt, compressionType)
      sizesByteBuffer.rewind()
      val sizes = LinkedList<Int>()

      repeat(numRecords) {
        sizes.add(sizesByteBuffer.getVarInt64().toInt())
      }

      val chunkBeginningSize = dataBuffer.position() - startingPoint
      val encodedDataSize = (this.dataSize.toLong(ByteOrder.LITTLE_ENDIAN) - chunkBeginningSize).toInt()

      val valuesByteBuffer = readCompressedBuffer(dataBuffer, encodedDataSize, compressionType)
      valuesByteBuffer.rewind()

      val records = LinkedList<ByteString>()

      for (size in sizes) {
        val recordByteBuffer = ByteBuffer.allocate(size)

        while(recordByteBuffer.hasRemaining()) {
          recordByteBuffer.put(valuesByteBuffer.get())
        }
        recordByteBuffer.rewind()

        records.add(recordByteBuffer.toByteString())
      }

      return records
    }
  }

  /**
   * Represents the contents of a block header within a Riegeli compressed file.
   *
   * The full specification of a block header is provided here:
   *   https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md#block-header
   *
   * @param headerHash      ByteString containing a HighwayHash of the other block header elements
   *                        (previousChunk and nextChunk).
   * @param previousChunk   ByteString representing the distance from the beginning of the chunk interrupted by this
   *                        block header to the beginning of the block. The ByteString should be storing a 64-bit,
   *                        little-endian unsigned integer.
   * @param nextChunk       ByteString representing the distance from the beginning of the block to the end of the chunk
   *                        interrupted by this block header. The ByteString should be storing a 64-bit, little-endian
   *                        unsigned integer.
   * @throws IllegalArgumentException   if the block header does not hash properly, meaning that the block header
   *                                    is corrupted.
   */
  private class BlockHeader private constructor(val headerHash: ByteString, val previousChunk: ByteString, val nextChunk: ByteString) {

    init {
      require(isValid(headerHash, previousChunk, nextChunk)) {
        "Riegeli: Block header is invalid"
      }
    }

    companion object {

      /**
       * Constructs and returns a new BlockHeader object from an input stream.
       *
       * Validates the block header its hash.
       *
       * @param inputStream Counting input stream from which the chunk should be read. The input stream's count must
       *                    have started at the beginning of a file.
       * @return A new BlockHeader object as read from the inputStream.
       */
      fun readFrom(inputStream: InputStream): BlockHeader {
        val headerHashArray = ByteArray(8)
        val previousChunkArray = ByteArray(8)
        val nextChunkArray = ByteArray(8)

        inputStream.read(headerHashArray)
        inputStream.read(previousChunkArray)
        inputStream.read(nextChunkArray)

        val headerHash = headerHashArray.toByteString()
        val previousChunk = previousChunkArray.toByteString()
        val nextChunk = nextChunkArray.toByteString()

        return BlockHeader(headerHash, previousChunk, nextChunk)
      }

      /**
       * Helper function for the block header builder function.
       * Compares the headerHash element of the block's header to a highway hash of the block's other header elements.
       *
       * @param headerHash      ByteString containing a HighwayHash of the other block header elements (previousChunk
       *                        and nextChunk).
       * @param previousChunk   ByteString representing the distance from the beginning of the chunk interrupted by this
       *                        block header to the beginning of the block. The ByteString should be storing a 64-bit,
       *                        little-endian unsigned integer.
       * @param nextChunk       ByteString representing the distance from the beginning of the block to the end of the
       *                        chunk interrupted by this block header. The ByteString should be storing a 64-bit,
       *                        little-endian unsigned integer.
       * @return True if the block's header is valid, false if it is not.
       */
      private fun isValid(headerHash: ByteString, previousChunk: ByteString, nextChunk: ByteString): Boolean {
        val highwayHash = buildHighwayHash()

        val headerData = previousChunk + nextChunk

        highwayHash.updateRemainder(headerData.toByteArray(), 0, headerData.size)

        val hashedDataLong = highwayHash.finalize64()

        val headerHashLong = headerHash.toLong(ByteOrder.LITTLE_ENDIAN)

        return hashedDataLong == headerHashLong
      }
    }
  }
}

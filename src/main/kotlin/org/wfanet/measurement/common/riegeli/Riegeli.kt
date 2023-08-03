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
import java.util.LinkedList
import java.util.logging.Logger
import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.commons.compress.utils.CountingInputStream
import org.wfanet.measurement.common.getVarInt
import org.wfanet.measurement.common.readLittleEndian56Int
import org.wfanet.measurement.common.readLittleEndian64Int
import org.wfanet.measurement.common.readLittleEndian64Long




// Package-level constant representing the fixed-size of a block within a Riegeli compressed file.
const val kBlockSize = 1 shl 16

fun readInputStreamIntoByteBuffer(inputStream: InputStream, byteBuffer: ByteBuffer): Boolean {
  while (byteBuffer.hasRemaining()) {
    val temp = inputStream.read()

    if (temp == -1) {
      //There is no more to read
      return false
    }

    byteBuffer.put(temp.toByte())

  }

  return true
}

/**
 * Generates a HighwayHash object with the key
 * 'Riegeli/', 'records\n', 'Riegeli/', 'records\n'
 *
 * @return A HighwayHash object with specified key.
 */
fun generateHighwayHash(): HighwayHash {
  return HighwayHash(0x2f696c6567656952, 0x0a7364726f636572, 0x2f696c6567656952, 0x0a7364726f636572)
}

/**
 * Native kotlin implementation of a Riegeli decompressor.
 *
 * Follows the Riegeli/Records standard as specified here:
 * https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md
 */
class Riegeli {

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
   * @param headerHash      64-bit ByteArray containing a HighwayHash of the other chunk header elements
   *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
   * @param dataSize        ByteArray representing the size of the chunk data, excluding intervening block headers,
   *                        as a 64-bit, little-endian long.
   * @param dataHash        ByteArray containing a HighwayHash of the data within the chunk.
   * @param chunkType       ByteArray containing a single byte which represents the chunk type.
   *                          - If chunkType is 0x6d, the chunk is a file metadata chunk.
   *                          - If chunkType is 0x70, the chunk is a padding chunk.
   *                          - If chunkType is 0x72, the chunk is a simple chunk with records.
   *                          - if chunkType is 0x74, the chunk is a transposed chunk with records.
   * @param numRecords      ByteArray representing the number of records inside the chunk as a 64-bit,
   *                        little-endian long.
   * @param decodedDataSize ByteArray representing the size of the chunk's data after it has been decoded as a 64-bit,
   *                        little-endian long
   * @param data            ByteArray with the chunk's compressed data. The array's size is represented by dataSize.
   */
  private class Chunk private constructor(val headerHash: ByteString,
                                          val dataSize: ByteString,
                                          val dataHash: ByteString,
                                          val chunkType: ByteString,
                                          val numRecords: ByteString,
                                          val decodedDataSize: ByteString,
                                          val data: ByteBuffer) {


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
       * @throws Exception if the chunk's header does not hash properly, meaning that the chunk header is corrupted.
       * @throws Exception if the hash of the chunk's data does not match dataHash within the chunk header, meaning
       *         the chunk's data is corrupted.
       * @return A new chunk object as read from the inputStream.
       */
      fun fromInputStream(inputStream: CountingInputStream): Chunk? {

        val headerHashArray = ByteArray(8)
        val dataSizeArray = ByteArray(8)
        val dataHashArray = ByteArray(8)
        val chunkTypeArray = ByteArray(1)
        val numRecordsArray = ByteArray(7)
        val decodedDataSizeArray = ByteArray(8)

        if (inputStream.read(headerHashArray) == -1) {
          //There is no more to read
          return null
        }

        inputStream.read(dataSizeArray)
        inputStream.read(dataHashArray)
        inputStream.read(chunkTypeArray)
        inputStream.read(numRecordsArray)
        inputStream.read(decodedDataSizeArray)

        val headerHash = headerHashArray.toByteString()
        val dataSize = dataSizeArray.toByteString()
        val dataHash = dataHashArray.toByteString()
        val chunkType = chunkTypeArray.toByteString()
        val numRecords = numRecordsArray.toByteString()
        val decodedDataSize = decodedDataSizeArray.toByteString()



        if (!isValidHeader(headerHash, dataSize, dataHash, chunkType, numRecords, decodedDataSize)) {
          throw Exception("Riegeli: Chunk header is invalid")
        }

        val dataStream = ByteArrayOutputStream()
        val dataSizeInt = readLittleEndian64Int(dataSize)

        repeat(dataSizeInt) {

          if (inputStream.bytesRead % kBlockSize == 0L) {
            BlockHeader.fromInputStream(inputStream)
          }

          dataStream.write(inputStream.read())
        }

        val data = ByteBuffer.wrap(dataStream.toByteArray())

        if (!isValidData(dataSize, dataHash, data)) {
          throw Exception("Riegeli: Chunk data is invalid")
        }

        return Chunk(headerHash, dataSize, dataHash, chunkType, numRecords, decodedDataSize, data)
      }

      /**
       * Helper function for the chunk builder function.
       * Compares the headerHash element of the chunk's header to a highway hash of the chunk's other header elements.
       *
       * @param headerHash      64-bit ByteArray containing a HighwayHash of the other chunk header elements
       *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
       * @param dataSize        ByteArray representing the size of the chunk data, excluding intervening block headers,
       *                        as a 64-bit, little-endian long.
       * @param dataHash        ByteArray containing a HighwayHash of the data within the chunk.
       * @param chunkType       ByteArray containing a single byte which represents the chunk type.
       * @param numRecords      ByteArray representing the number of records inside the chunk as a 64-bit,
       *                        little-endian long.
       * @param decodedDataSize ByteArray representing the size of the chunk's data after it has been decoded as a
       *                        64-bit, little-endian long
       * @return True if the chunk's header is valid, false if it is not.
       */
      private fun isValidHeader(headerHash: ByteString,
                                dataSize: ByteString,
                                dataHash: ByteString,
                                chunkType: ByteString,
                                numRecords: ByteString,
                                decodedDataSize: ByteString): Boolean {

        val highwayHash = generateHighwayHash()

        val headerData = dataSize + dataHash + chunkType + numRecords + decodedDataSize

        highwayHash.updatePacket(headerData.toByteArray(), 0)

        val hashedDataLong = highwayHash.finalize64()

        val headerHashLong = readLittleEndian64Long(headerHash)

        return hashedDataLong == headerHashLong
      }

      /**
       * Helper function for the chunk builder function.
       * Compares the dataHash element of the chunk's header to a highway hash of the chunk's data.
       *
       * @param dataSize  ByteArray representing the size of the chunk data, excluding intervening block headers,
       *                  as a 64-bit, little-endian long.
       * @param dataHash  ByteArray containing a HighwayHash of the data within the chunk.
       * @param data      ByteArray with the chunk's compressed data. The array's size is represented by dataSize.
       * @return True if the chunk's header is valid, false if it is not.
       */
      private fun isValidData(dataSize: ByteString, dataHash: ByteString, data: ByteBuffer): Boolean {
        val highwayHash = generateHighwayHash()

        var position = 0
        val dataSizeInt = readLittleEndian64Int(dataSize)

        while (dataSizeInt - position >= 32) {
          highwayHash.updatePacket(data.array(), position)
          position += 32
        }

        if (dataSizeInt - position > 0) {
          highwayHash.updateRemainder(data.array(), position, dataSizeInt - position)
        }

        val dataHashLong = readLittleEndian64Long(dataHash)
        val hashedDataLong = highwayHash.finalize64()

        return dataHashLong == hashedDataLong
      }
    }

    /**
     * Reads a compressed buffer from a chunk with records.
     *
     * Compressed buffers, if compressionType is not 0, are prefixed with a varint containing their decompressed size.
     *
     * @param inputStream     The input stream from which the buffer should be read.
     * @param compressedSize  The size of the buffer, the number of bytes that should be read from the input stream.
     * @param compressionType A Byte representing the type of compression used in the compressed buffer.
     *                          - 0x00: none
     *                          - 0x62: Brotli
     *                          - 0x7a: Zstd
     *                          - 0x73: Snappy
     * @return A ByteArray containing the decompressed data from the buffer.
     */
    private fun readCompressedBuffer(dataBuffer: ByteBuffer, compressedSize: Int, compressionType: Byte): ByteBuffer {
      val startingPoint = dataBuffer.position()

      println("STARTING POINT: ${startingPoint}")

      //If compression type is 0, there is not a varint at the beginning of the buffer so do not read it.
      val decompressedSize = if (compressionType == 0x00.toByte()) -1 else getVarInt(dataBuffer)

      val sizeOfVarInt = (dataBuffer.position() - startingPoint).toInt()

      println("Size of VARINT: ${sizeOfVarInt}")
      println("Ending POINT: ${dataBuffer.position()}")
      println("DEcompressed Size: ${decompressedSize}")

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

      val uncompressedBuffer = ByteBuffer.allocate(decompressedSize)

      readInputStreamIntoByteBuffer(compressedInputStream, uncompressedBuffer)

      return uncompressedBuffer
    }

    /**
     * Gets the records stored within the chunk as a list of ByteArrays.
     *
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
     * @return  A list where each element is a ByteArray representing a decompressed record. If there are no records
     *          within the chunk, an empty list is returned.
     */
    fun getRecords(): LinkedList<ByteString> {

      //If the chunk is not a chunk with records, return an empty list
      if (this.chunkType.byteAt(0) != 0x72.toByte()) {
        return LinkedList<ByteString>()
      }

      val dataBuffer = this.data.duplicate().rewind()

      val startingPoint = dataBuffer.position()

      val compressionType = dataBuffer.get()

      val compressedSizesSizeInt = getVarInt(dataBuffer)

      val numRecords = readLittleEndian56Int(this.numRecords)

      val sizesByteBuffer = readCompressedBuffer(dataBuffer, compressedSizesSizeInt, compressionType)
      sizesByteBuffer.rewind()
      val sizes = LinkedList<Int>()

      repeat(numRecords) {
        sizes.add(getVarInt(sizesByteBuffer))
      }

      val chunkBeginningSize = dataBuffer.position() - startingPoint
      val encodedDataSize = (readLittleEndian64Long(this.dataSize) - chunkBeginningSize).toInt()

      val valuesByteBuffer = readCompressedBuffer(dataBuffer, encodedDataSize, compressionType)
      valuesByteBuffer.rewind()

      val records = LinkedList<ByteString>()

      for (size in sizes) {
        val recordByteBuffer = ByteBuffer.allocate(size)

        while(recordByteBuffer.hasRemaining()) {
          recordByteBuffer.put(valuesByteBuffer.get())
        }

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
   * @param headerHash      64-bit ByteArray containing a HighwayHash of the other block header elements
   *                        (previousChunk and nextChunk).
   * @param previousChunk   ByteArray representing the distance from the beginning of the chunk interrupted by this
   *                        block header to the beginning of the block as a 64-bit, little-endian long.
   * @param nextChunk       ByteArray representing the distance from the beginning of the block to the end of the chunk
   *                        interrupted by this block header as a 64-bit, little-endian long.
   */
  private class BlockHeader private constructor(val headerHash: ByteString, val previousChunk: ByteString, val nextChunk: ByteString) {

    companion object {

      /**
       * Constructs and returns a new BlockHeader object from an input stream.
       *
       * Validates the block header its hash.
       *
       * @param inputStream Counting input stream from which the chunk should be read. The input stream's count must
       *                    have started at the beginning of a file.
       * @throws Exception if the block header does not hash properly, meaning that the block header is corrupted.
       * @return A new BlockHeader object as read from the inputStream.
       */
      fun fromInputStream(inputStream: InputStream): BlockHeader {
        val headerHashArray = ByteArray(8)
        val previousChunkArray = ByteArray(8)
        val nextChunkArray = ByteArray(8)

        inputStream.read(headerHashArray)
        inputStream.read(previousChunkArray)
        inputStream.read(nextChunkArray)

        val headerHash = headerHashArray.toByteString()
        val previousChunk = previousChunkArray.toByteString()
        val nextChunk = nextChunkArray.toByteString()

        if (!this.isValid(headerHash, previousChunk, nextChunk)) {
          throw Exception("Riegeli: Block header is invalid")
        }

        return BlockHeader(headerHash, previousChunk, nextChunk)
      }

      /**
       * Helper function for the block header builder function.
       * Compares the headerHash element of the block's header to a highway hash of the block's other header elements.
       *
       * @param headerHash      64-bit ByteArray containing a HighwayHash of the other block header elements
       *                        (previousChunk and nextChunk).
       * @param previousChunk   ByteArray representing the distance from the beginning of the chunk interrupted by this
       *                        block header to the beginning of the block as a 64-bit, little-endian long.
       * @param nextChunk       ByteArray representing the distance from the beginning of the block to the end of the chunk
       *                        interrupted by this block header as a 64-bit, little-endian long.
       * @return True if the block's header is valid, false if it is not.
       */
      private fun isValid(headerHash: ByteString, previousChunk: ByteString, nextChunk: ByteString): Boolean {
        val highwayHash = generateHighwayHash()

        val headerData = previousChunk + nextChunk

        highwayHash.updateRemainder(headerData.toByteArray(), 0, headerData.size)

        val hashedDataLong = highwayHash.finalize64()

        val headerHashLong = readLittleEndian64Long(headerHash)

        return hashedDataLong == headerHashLong
      }
    }
  }

  /**
   * Reads and decompresses a Riegeli compressed input stream with records.
   *
   * @param incomingInputStream An input stream which contains data which should be decompressed using Riegeli.
   * @return A list where each element is a ByteArray containing the bytes of a record.
   */
  fun readCompressedInputStreamWithRecords(incomingInputStream: InputStream): List<ByteString> {
    val inputStream = CountingInputStream(incomingInputStream)
    BlockHeader.fromInputStream(inputStream)

    Chunk.fromInputStream(inputStream)
    val records = LinkedList<ByteString>()

    while (true) {
      val chunk = Chunk.fromInputStream(inputStream) ?: break

      //Chunk type is simple chunk with records (0x72)
      if (chunk.chunkType.byteAt(0) == 0x72.toByte()) {
        records.addAll(chunk.getRecords())
      } else {
        //Ignored/unsupported chunk types:
        //0x73 - File Signature - Present at the beginning of the file, encodes no records, is ignored
        //0x6d - File Metadata - provides information describing the records, not necessary to read, is ignored
        //0x70 - Padding - encodes no records and only occupies file space, is ignored
        //0x74 - Transposed Chunk with Records - no documentation provided for the format of this chunk, unimplementable

        logger.info("NON RECORD CHUNK -- Chunk type: ${chunk.chunkType.byteAt(0)}")
      }
    }

    return records
  }

  /**
   * Reads and decompresses a Riegeli compressed file with records.
   *
   * @param filename The path of the file which should be decompressed using Riegeli.
   * @return A list where each element is a ByteArray containing the bytes of a record.
   */
  fun readCompressedFileWithRecords(filename: String): List<ByteString> {
    val inputStream = File(filename).inputStream()

    val list = readCompressedInputStreamWithRecords(inputStream)

    inputStream.close()

    return list
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

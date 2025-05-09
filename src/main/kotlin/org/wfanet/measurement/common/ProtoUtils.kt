// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Duration as ProtoDuration
import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.ProtocolMessageEnum
import com.google.protobuf.TextFormat
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.Interval
import com.google.type.interval
import java.io.File
import java.time.Clock
import java.time.Duration
import java.time.Instant

/** Converts a protobuf [MessageOrBuilder] into its canonical JSON representation. */
fun MessageOrBuilder.toJson(): String {
  return JsonFormat.printer().omittingInsignificantWhitespace().print(this)
}

/**
 * Truncates all of the [bytes][FieldDescriptor.Type.BYTES] fields in this [Message.Builder]
 * in-place, returning itself for chaining.
 *
 * @param truncatedSize the size in bytes to truncate to
 */
fun <T : Message.Builder> T.truncateByteFields(truncatedSize: Int): T {
  descriptors@ for (descriptor in descriptorForType.fields) {
    when (descriptor.type) {
      FieldDescriptor.Type.BYTES -> {
        if (descriptor.isRepeated) {
          val fields = getField(descriptor) as List<*>
          fields.forEachIndexed { index, value ->
            val bytes = value as ByteString
            if (bytes.size() > truncatedSize) {
              setRepeatedField(descriptor, index, bytes.substring(0, truncatedSize))
            }
          }
        } else {
          val bytes = getField(descriptor) as ByteString
          if (bytes.size() > truncatedSize) {
            setField(descriptor, bytes.substring(0, truncatedSize))
          }
        }
      }
      FieldDescriptor.Type.MESSAGE -> {
        if (descriptor.isRepeated) {
          val fields = getField(descriptor) as List<*>
          fields.forEachIndexed { index, field ->
            val message = field as Message
            setRepeatedField(descriptor, index, message.truncateByteFields(truncatedSize))
          }
        } else {
          if (!hasField(descriptor)) {
            // Skip unset fields. This also avoids clobbering oneofs.
            continue@descriptors
          }

          val message = getField(descriptor) as Message
          setField(descriptor, message.truncateByteFields(truncatedSize))
        }
      }
      else -> {} // No-op.
    }
  }

  return this
}

/** Truncate all byte fields inside a protobuf [Message]. */
fun <T : Message> T.truncateByteFields(truncatedSize: Int): T {
  @Suppress("UNCHECKED_CAST") // Safe due to Message contract.
  return toBuilder().truncateByteFields(truncatedSize).build() as T
}

fun Instant.toProtoTime(): Timestamp = timestamp {
  seconds = epochSecond
  nanos = nano
}

fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

fun Duration.toProtoDuration(): ProtoDuration {
  val source = this
  return duration {
    seconds = source.seconds
    nanos = nano
  }
}

fun ProtoDuration.toDuration(): Duration = Duration.ofSeconds(seconds, nanos.toLong())

fun Clock.protoTimestamp(): Timestamp = instant().toProtoTime()

/** Converts this [Interval] to an [OpenEndTimeRange]. */
fun Interval.toRange(): OpenEndTimeRange =
  OpenEndTimeRange(startTime.toInstant(), endTime.toInstant())

/** Converts this [OpenEndTimeRange] to an [Interval]. */
fun OpenEndTimeRange.toInterval(): Interval = interval {
  startTime = start.toProtoTime()
  endTime = endExclusive.toProtoTime()
}

val ProtocolMessageEnum.numberAsLong: Long
  get() = number.toLong()

fun Message.Builder.mergeFromTextProto(textProto: Readable, typeRegistry: TypeRegistry) {
  TextFormat.Parser.newBuilder().setTypeRegistry(typeRegistry).build().merge(textProto, this)
}

@Suppress("UNCHECKED_CAST") // Safe per Message contract.
fun <T : Message> parseTextProto(
  textProto: Readable,
  messageInstance: T,
  typeRegistry: TypeRegistry = TypeRegistry.getEmptyTypeRegistry(),
): T {
  return messageInstance
    .newBuilderForType()
    .apply { mergeFromTextProto(textProto, typeRegistry) }
    .build() as T
}

fun <T : Message> parseTextProto(
  textProto: File,
  messageInstance: T,
  typeRegistry: TypeRegistry = TypeRegistry.getEmptyTypeRegistry(),
): T {
  return textProto.bufferedReader().use { reader ->
    parseTextProto(reader, messageInstance, typeRegistry)
  }
}

/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.Timestamp
import com.google.protobuf.TimestampProto
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.timestamp
import java.time.Clock
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfa.measurement.common.testing.ContainsAnyOuterClass.ContainsAny
import org.wfa.measurement.common.testing.containsAny
import org.wfanet.measurement.common.ProtoReflection.allDependencies
import org.wfanet.measurement.common.testing.DependsOnSimple
import org.wfanet.measurement.common.testing.DependsOnSimpleKt
import org.wfanet.measurement.common.testing.Sibling
import org.wfanet.measurement.common.testing.Simple
import org.wfanet.measurement.common.testing.dependsOnSimple
import org.wfanet.measurement.common.testing.simple

/* Test for [ProtoReflection]. */
@RunWith(JUnit4::class)
class ProtoReflectionTest {
  @Test
  fun `allDependencies returns all dependencies`() {
    val dependencies: Set<Descriptors.FileDescriptor> =
      DependsOnSimple.getDescriptor().file.allDependencies

    assertThat(dependencies)
      .containsExactly(TimestampProto.getDescriptor(), Simple.getDescriptor().file)
  }

  @Test
  fun `buildFileDescriptorSet excludes well-known types`() {
    val fileDescriptorSet: DescriptorProtos.FileDescriptorSet =
      ProtoReflection.buildFileDescriptorSet(DependsOnSimple.getDescriptor())

    assertThat(fileDescriptorSet.fileList)
      .containsExactly(
        DependsOnSimple.getDescriptor().file.toProto(),
        Simple.getDescriptor().file.toProto(),
      )
  }

  @Test
  fun `buildFileDescriptors builds FileDescriptors from set`() {
    val fileDescriptorSet: DescriptorProtos.FileDescriptorSet =
      ProtoReflection.buildFileDescriptorSet(DependsOnSimple.getDescriptor())

    val fileDescriptors: List<Descriptors.FileDescriptor> =
      ProtoReflection.buildFileDescriptors(listOf(fileDescriptorSet))

    val fileNames = fileDescriptors.map { it.fullName }
    assertThat(fileNames)
      .containsExactly(
        DependsOnSimple.getDescriptor().file.fullName,
        Simple.getDescriptor().file.fullName,
      )
  }

  @Test
  fun `buildDescriptors builds Descriptors from set`() {
    val fileDescriptorSet: DescriptorProtos.FileDescriptorSet =
      ProtoReflection.buildFileDescriptorSet(DependsOnSimple.getDescriptor())

    val descriptors: List<Descriptors.Descriptor> =
      ProtoReflection.buildDescriptors(listOf(fileDescriptorSet))

    val descriptorNames = descriptors.map { it.fullName }
    assertThat(descriptorNames)
      .containsExactly(
        Simple.getDescriptor().fullName,
        DependsOnSimple.getDescriptor().fullName,
        Sibling.getDescriptor().fullName,
      )
  }

  @Test
  fun `getDefaultInstance returns default instance`() {
    val timestamp: Timestamp = ProtoReflection.getDefaultInstance()

    assertThat(timestamp).isEqualToDefaultInstance()
  }

  @Test
  fun `pack packs message into Any`() {
    val message: Timestamp = Clock.systemUTC().protoTimestamp()

    val packed: ProtoAny = message.pack()

    assertThat(packed.unpack<Timestamp>()).isEqualTo(message)
  }

  @Test
  fun `getTypeUrl returns type URL with default prefix`() {
    val message = Timestamp.getDefaultInstance()

    val typeUrl = ProtoReflection.getTypeUrl(message.descriptorForType)

    assertThat(typeUrl.split("/").first()).isEqualTo(ProtoReflection.DEFAULT_TYPE_URL_PREFIX)
    assertThat(typeUrl).isEqualTo(message.pack().typeUrl)
  }

  @Test
  fun `getTypeUrl returns type URL with specified prefix`() {
    val message = Timestamp.getDefaultInstance()
    val typeUrlPrefix = "type.example.com"

    val typeUrl = ProtoReflection.getTypeUrl(message.descriptorForType, typeUrlPrefix)

    assertThat(typeUrl.split("/"))
      .containsExactly(typeUrlPrefix, "google.protobuf.Timestamp")
      .inOrder()
  }

  @Test
  fun `getFieldsRecursive returns fields`() {
    val embeddedMessage = dependsOnSimple {
      simple = simple { intValue = 1 }
      timestamp = timestamp {
        seconds = 1754011048
        nanos = 402358122
      }
      nested = DependsOnSimpleKt.nested { nestedIntValue = 2 }
    }
    val embeddedMessage2 = simple { intValue = 3 }
    val message = containsAny {
      anyValue += ProtoAny.pack(embeddedMessage)
      anyValue += ProtoAny.pack(embeddedMessage2)
    }
    val typeRegistry = TypeRegistry.newBuilder().add(DependsOnSimple.getDescriptor()).build()

    val fields: Sequence<ProtoReflection.Field> =
      ProtoReflection.getFieldsRecursive(message, typeRegistry)

    val anyValuePath =
      listOf(
        ProtoReflection.FieldPathSegment(
          ContainsAny.getDescriptor().findFieldByNumber(ContainsAny.ANY_VALUE_FIELD_NUMBER)
        )
      )
    val embeddedMessagePath = anyValuePath.map { it.withIndex(0) }
    val dependsOnSimpleDescriptor = DependsOnSimple.getDescriptor()
    val simplePath =
      embeddedMessagePath +
        ProtoReflection.FieldPathSegment(
          dependsOnSimpleDescriptor.findFieldByNumber(DependsOnSimple.SIMPLE_FIELD_NUMBER)
        )
    val intValuePath =
      simplePath +
        ProtoReflection.FieldPathSegment(
          Simple.getDescriptor().findFieldByNumber(Simple.INT_VALUE_FIELD_NUMBER)
        )
    val timestampPath =
      embeddedMessagePath +
        ProtoReflection.FieldPathSegment(
          dependsOnSimpleDescriptor.findFieldByNumber(DependsOnSimple.TIMESTAMP_FIELD_NUMBER)
        )
    val secondsPath =
      timestampPath +
        ProtoReflection.FieldPathSegment(
          Timestamp.getDescriptor().findFieldByNumber(Timestamp.SECONDS_FIELD_NUMBER)
        )
    val nanosPath =
      timestampPath +
        ProtoReflection.FieldPathSegment(
          Timestamp.getDescriptor().findFieldByNumber(Timestamp.NANOS_FIELD_NUMBER)
        )
    val nestedPath =
      embeddedMessagePath +
        ProtoReflection.FieldPathSegment(
          dependsOnSimpleDescriptor.findFieldByNumber(DependsOnSimple.NESTED_FIELD_NUMBER)
        )
    val nestedIntPath =
      nestedPath +
        ProtoReflection.FieldPathSegment(
          DependsOnSimple.Nested.getDescriptor()
            .findFieldByNumber(DependsOnSimple.Nested.NESTED_INT_VALUE_FIELD_NUMBER)
        )
    val siblingPath =
      embeddedMessagePath +
        ProtoReflection.FieldPathSegment(
          dependsOnSimpleDescriptor.findFieldByNumber(DependsOnSimple.SIBLING_FIELD_NUMBER)
        )
    val siblingIntPath =
      siblingPath +
        ProtoReflection.FieldPathSegment(
          Sibling.getDescriptor().findFieldByNumber(Sibling.SIBLING_INT_VALUE_FIELD_NUMBER)
        )
    val embeddedMessage2Path = embeddedMessagePath.map { it.withIndex(1) }
    val int2Path = embeddedMessage2Path + intValuePath.last()
    assertThat(fields.toList())
      .containsExactly(
        ProtoReflection.Field(message, anyValuePath, message.anyValueList),
        ProtoReflection.Field(embeddedMessage, simplePath, embeddedMessage.simple),
        ProtoReflection.Field(
          embeddedMessage.simple,
          intValuePath,
          embeddedMessage.simple.intValue,
        ),
        ProtoReflection.Field(embeddedMessage, timestampPath, embeddedMessage.timestamp),
        ProtoReflection.Field(
          embeddedMessage.timestamp,
          secondsPath,
          embeddedMessage.timestamp.seconds,
        ),
        ProtoReflection.Field(
          embeddedMessage.timestamp,
          nanosPath,
          embeddedMessage.timestamp.nanos,
        ),
        ProtoReflection.Field(embeddedMessage, nestedPath, embeddedMessage.nested),
        ProtoReflection.Field(
          embeddedMessage.nested,
          nestedIntPath,
          embeddedMessage.nested.nestedIntValue,
        ),
        ProtoReflection.Field(embeddedMessage, siblingPath, embeddedMessage.sibling),
        ProtoReflection.Field(
          embeddedMessage.sibling,
          siblingIntPath,
          embeddedMessage.sibling.siblingIntValue,
        ),
        ProtoReflection.Field(embeddedMessage2, int2Path, embeddedMessage2.intValue),
      )
  }
}

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
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.Timestamp
import com.google.protobuf.TimestampProto
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ProtoReflection.allDependencies
import org.wfanet.measurement.common.testing.DependsOnSimple
import org.wfanet.measurement.common.testing.Sibling
import org.wfanet.measurement.common.testing.Simple

/* Test for [ProtoReflection]. */
@RunWith(JUnit4::class)
class ProtoReflectionTest {
  @Test
  fun `allDependencies returns all dependencies`() {
    val dependencies: Set<Descriptors.FileDescriptor> =
      DependsOnSimple.getDescriptor().file.allDependencies

    assertThat(dependencies)
      .containsExactly(
        TimestampProto.getDescriptor(),
        Simple.getDescriptor().file,
      )
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
  fun `buildDescriptors builds descriptors from set`() {
    val fileDescriptorSet: DescriptorProtos.FileDescriptorSet =
      ProtoReflection.buildFileDescriptorSet(DependsOnSimple.getDescriptor())

    val descriptors = ProtoReflection.buildDescriptors(listOf(fileDescriptorSet))

    val descriptorNames = descriptors.map { it.fullName }
    assertThat(descriptorNames)
      .containsExactly(
        Simple.getDescriptor().fullName,
        Timestamp.getDescriptor().fullName,
        DependsOnSimple.getDescriptor().fullName,
        Sibling.getDescriptor().fullName,
      )
  }
}

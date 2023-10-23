/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.AnyProto
import com.google.protobuf.ApiProto
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DurationProto
import com.google.protobuf.EmptyProto
import com.google.protobuf.Message
import com.google.protobuf.StructProto
import com.google.protobuf.TimestampProto
import com.google.protobuf.TypeProto
import com.google.protobuf.WrappersProto
import com.google.protobuf.fileDescriptorSet
import kotlin.reflect.KClass
import kotlin.reflect.full.staticFunctions

/** Utility object for protobuf reflection. */
object ProtoReflection {
  /** Default type URL prefix (without a trailing `/`). */
  const val DEFAULT_TYPE_URL_PREFIX = "type.googleapis.com"

  /**
   * [Descriptors.FileDescriptor]s of
   * [well-known types](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf)
   * .
   */
  val WELL_KNOWN_TYPES: List<Descriptors.FileDescriptor> =
    listOf(
      TypeProto.getDescriptor(),
      DescriptorProtos.getDescriptor(),
      WrappersProto.getDescriptor(),
      AnyProto.getDescriptor(),
      ApiProto.getDescriptor(),
      DurationProto.getDescriptor(),
      EmptyProto.getDescriptor(),
      StructProto.getDescriptor(),
      TimestampProto.getDescriptor(),
    )

  private val WELL_KNOWN_TYPES_BY_NAME = WELL_KNOWN_TYPES.associateBy { it.name }

  /** Returns the type URL for the specified message type. */
  fun getTypeUrl(
    descriptor: Descriptors.Descriptor,
    typeUrlPrefix: String = DEFAULT_TYPE_URL_PREFIX
  ): String {
    return getTypeUrl(descriptor.fullName, typeUrlPrefix)
  }

  /**
   * Returns the type URL for the specified message type.
   *
   * Prefer the other overload when a [Descriptors.Descriptor] is available.
   */
  fun getTypeUrl(fullName: String, typeUrlPrefix: String = DEFAULT_TYPE_URL_PREFIX): String {
    return if (typeUrlPrefix.endsWith("/")) {
      typeUrlPrefix + fullName
    } else {
      "$typeUrlPrefix/$fullName"
    }
  }

  /** Reflectively calls the `getDefaultInstance` static function for [T]. */
  inline fun <reified T : Message> getDefaultInstance(): T {
    return getDefaultInstance(T::class)
  }

  /** Reflectively calls the `getDefaultInstance` static function for [T]. */
  fun <T : Message> getDefaultInstance(kclass: KClass<T>): T {
    // Every Message type should have a static getDefaultInstance function.
    @Suppress("UNCHECKED_CAST") // Guaranteed by predicate.
    val function =
      kclass.staticFunctions.single { it.name == "getDefaultInstance" && it.parameters.isEmpty() }
        as kotlin.reflect.KFunction0<T>

    return function.call()
  }

  /**
   * Builds a [DescriptorProtos.FileDescriptorSet] from [descriptor], including direct and
   * transitive dependencies.
   *
   * [Descriptors.FileDescriptor]s of [WELL_KNOWN_TYPES] are excluded from the output.
   */
  fun buildFileDescriptorSet(
    descriptor: Descriptors.Descriptor,
    knownTypes: Iterable<Descriptors.FileDescriptor> = WELL_KNOWN_TYPES
  ): DescriptorProtos.FileDescriptorSet {
    val fileDescriptors = mutableSetOf<Descriptors.FileDescriptor>()
    val rootFileDescriptor: Descriptors.FileDescriptor = descriptor.file
    fileDescriptors.addDeps(rootFileDescriptor)
    fileDescriptors.add(rootFileDescriptor)

    val knownTypesByName: Map<String, Descriptors.FileDescriptor> = knownTypes.byName()
    return fileDescriptorSet {
      for (fileDescriptor in fileDescriptors) {
        if (knownTypesByName.containsKey(fileDescriptor.name)) {
          continue
        }
        this.file += fileDescriptor.toProto()
      }
    }
  }

  /** Direct and transitive dependencies of this [Descriptors.FileDescriptor]. */
  val Descriptors.FileDescriptor.allDependencies: Set<Descriptors.FileDescriptor>
    get() {
      return mutableSetOf<Descriptors.FileDescriptor>().also { it.addDeps(this) }
    }

  /** Adds all direct and transitive dependencies of [fileDescriptor] to this [MutableSet]. */
  private fun MutableSet<Descriptors.FileDescriptor>.addDeps(
    fileDescriptor: Descriptors.FileDescriptor
  ) {
    for (dep in fileDescriptor.dependencies) {
      if (contains(dep)) {
        continue
      }
      addDeps(dep)
      add(dep)
    }
  }

  /** Builds [Descriptors.Descriptor]s from [fileDescriptorSets]. */
  fun buildDescriptors(
    fileDescriptorSets: Iterable<DescriptorProtos.FileDescriptorSet>,
    knownTypes: Iterable<Descriptors.FileDescriptor> = WELL_KNOWN_TYPES
  ): List<Descriptors.Descriptor> {
    val knownTypesByName: Map<String, Descriptors.FileDescriptor> = knownTypes.byName()
    val fileDescriptorsByName: Map<String, Descriptors.FileDescriptor> =
      FileDescriptorMapBuilder(
          fileDescriptorSets.flatMap { it.fileList }.associateBy { it.name },
          knownTypesByName
        )
        .build()
    return fileDescriptorsByName.values.flatMap { it.messageTypes }
  }

  private class FileDescriptorMapBuilder(
    private val fileDescriptorProtos: Map<String, DescriptorProtos.FileDescriptorProto>,
    private val knownTypesByName: Map<String, Descriptors.FileDescriptor>
  ) {
    /** Builds a [Map] of file name to [Descriptors.FileDescriptor]. */
    fun build(): Map<String, Descriptors.FileDescriptor> {
      val fileDescriptors = mutableMapOf<String, Descriptors.FileDescriptor>()
      for (fileDescriptorProto in fileDescriptorProtos.values) {
        fileDescriptors.add(fileDescriptorProto)
      }
      return fileDescriptors
    }

    private fun MutableMap<String, Descriptors.FileDescriptor>.add(
      fileDescriptorProto: DescriptorProtos.FileDescriptorProto
    ) {
      if (containsKey(fileDescriptorProto.name)) {
        return
      }
      addDeps(fileDescriptorProto)
      put(
        fileDescriptorProto.name,
        Descriptors.FileDescriptor.buildFrom(
          fileDescriptorProto,
          fileDescriptorProto.dependencyList.map { getValue(it) }.toTypedArray()
        )
      )
    }

    /**
     * Adds all direct and transitive dependencies of [fileDescriptorProto] to this [MutableMap].
     */
    private fun MutableMap<String, Descriptors.FileDescriptor>.addDeps(
      fileDescriptorProto: DescriptorProtos.FileDescriptorProto,
    ) {
      for (depName in fileDescriptorProto.dependencyList) {
        if (containsKey(depName)) {
          continue
        }
        val knownType: Descriptors.FileDescriptor? = knownTypesByName[depName]
        if (knownType != null) {
          put(depName, knownType)
          continue
        }

        val depProto: DescriptorProtos.FileDescriptorProto = fileDescriptorProtos.getValue(depName)
        addDeps(depProto)
        put(
          depName,
          Descriptors.FileDescriptor.buildFrom(
            depProto,
            depProto.dependencyList.map { getValue(it) }.toTypedArray()
          )
        )
      }
    }
  }

  private fun Iterable<Descriptors.FileDescriptor>.byName():
    Map<String, Descriptors.FileDescriptor> {

    if (this === WELL_KNOWN_TYPES) {
      // Optimize for common case.
      return WELL_KNOWN_TYPES_BY_NAME
    }
    return associateBy { it.name }
  }
}

/** See [ProtoAny.pack]. */
fun Message.pack(): ProtoAny {
  return ProtoAny.pack(this)
}

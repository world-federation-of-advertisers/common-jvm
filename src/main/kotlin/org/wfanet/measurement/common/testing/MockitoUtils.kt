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

package org.wfanet.measurement.common.testing

import com.google.common.truth.extensions.proto.ProtoSubject
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.Message
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.verification.VerificationMode

/** Captures the sole parameter to [method] on a Mockito [mock]. */
inline fun <reified T : Any, M> verifyAndCapture(
  mock: M,
  crossinline method: suspend M.(T) -> Any
): T = captureFirst { verifyBlocking(mock) { this.method(capture()) } }

/** Creates a captor, runs [block] in its scope, and returns the first captured value. */
inline fun <reified T : Any> captureFirst(block: KArgumentCaptor<T>.() -> Unit): T =
  argumentCaptor(block).firstValue

/** Captures all values with a parameter [mode] */
inline fun <reified T, reified M> captureInMode(
  mock: M,
  noinline method: suspend M.(T) -> Any,
  mode: VerificationMode
): List<T> {
  val captor: KArgumentCaptor<T> = argumentCaptor()
  verifyBlocking(mock, mode) { method(captor.capture()) }
  return captor.allValues
}

/**
 * Captures the first argument to [method], a proto message, and runs [ProtoTruth.assertThat] on it
 * for convenient chaining.
 *
 * For example: verifyProtoArgument(someMock, SomeClass::someMethod)
 * ```
 *     .comparedExpectedFieldsOnly()
 *     .isEqualTo(someExpectedProto)
 * ```
 */
inline fun <reified T : Message, M> verifyProtoArgument(
  mock: M,
  noinline method: suspend M.(T) -> Any
): ProtoSubject {
  return ProtoTruth.assertThat(verifyAndCapture(mock, method))
}

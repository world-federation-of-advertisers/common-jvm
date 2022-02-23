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

package org.wfanet.measurement.common.grpc.testing

import io.grpc.kotlin.AbstractCoroutineServerImpl
import org.mockito.kotlin.KStubbing
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.mock

/** Creates a mock for a gRPC coroutine service [T], allowing for immediate stubbing. */
inline fun <reified T : AbstractCoroutineServerImpl> mockService(
  stubbing: KStubbing<T>.(T) -> Unit
): T =
  mock(useConstructor = UseConstructor.parameterless()) { mock ->
    on { context }.thenCallRealMethod()
    stubbing(mock)
  }

/** Creates a mock for a gRPC coroutine service [T]. */
inline fun <reified T : AbstractCoroutineServerImpl> mockService(): T =
  mock(useConstructor = UseConstructor.parameterless()) { on { context }.thenCallRealMethod() }

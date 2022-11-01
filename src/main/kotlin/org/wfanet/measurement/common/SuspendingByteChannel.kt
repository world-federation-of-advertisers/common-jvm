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

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.AsynchronousChannel
import kotlinx.coroutines.suspendCancellableCoroutine

/** Suspending wrapper around [delegate]. */
class SuspendingByteChannel(private val delegate: AsynchronousByteChannel) :
  AsynchronousChannel by delegate {

  /**
   * Reads bytes from the channel into [destination].
   *
   * @see AsynchronousByteChannel.read
   */
  suspend fun read(destination: ByteBuffer): Int {
    return delegate.readSuspending(destination)
  }

  /**
   * Writes bytes from [source] into the channel.
   *
   * @see AsynchronousByteChannel.write
   */
  suspend fun write(source: ByteBuffer): Int {
    return delegate.writeSuspending(source)
  }
}

/**
 * Reads bytes from the channel into [destination].
 *
 * @see AsynchronousByteChannel.read
 */
suspend fun AsynchronousByteChannel.readSuspending(destination: ByteBuffer): Int {
  return suspendCancellableCoroutine { continuation ->
    read(destination, continuation, intCompletionHandler)
  }
}

/**
 * Writes bytes from [source] into the channel.
 *
 * @see AsynchronousByteChannel.write
 */
suspend fun AsynchronousByteChannel.writeSuspending(source: ByteBuffer): Int {
  return suspendCancellableCoroutine { continuation ->
    write(source, continuation, intCompletionHandler)
  }
}

private val intCompletionHandler = ContinuationCompletionHandler<Int>()

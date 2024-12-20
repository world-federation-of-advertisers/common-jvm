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

package org.wfanet.measurement.common

import com.google.protobuf.ByteString

/** Representation of an uppercase hexidecimal [String]. */
data class HexString(val bytes: ByteString) {
  @OptIn(ExperimentalStdlibApi::class)
  val value: String
    get() = bytes.toByteArray().toHexString(HexFormat.UpperCase)

  @OptIn(ExperimentalStdlibApi::class)
  constructor(value: String) : this(ByteString.copyFrom(value.hexToByteArray()))

  override fun toString(): String = value
}

fun ByteString.toHexString() = HexString(this)

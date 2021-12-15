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

package org.wfanet.measurement.common.identity

import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.toLong

/**
 * Typesafe wrapper around Long to represent the integer format used below the service layer for the
 * internal representation of external identifiers.
 *
 * @property[value] a non-negative integer identifier greater than 0
 */
data class ExternalId(val value: Long) {
  init {
    require(value > 0) { "Negative id numbers and 0 are not permitted: $value" }
  }

  val apiId: ApiId by lazy { ApiId(value.base64UrlEncode()) }

  override fun toString(): String = "ExternalId($value / ${apiId.value})"
}

/**
 * Typesafe wrapper around String to represent the service-layer identifier format.
 *
 * This eagerly decodes [value] into an [ExternalId] to validate the base64 encoding.
 *
 * @property[value] the websafe base64 external identifier
 */
data class ApiId(val value: String) {
  val externalId: ExternalId = ExternalId(value.base64UrlDecode().toLong())
}

/** Convenience function to convert a String [ApiId] value to a Long [ExternalId] value. */
fun apiIdToExternalId(id: String): Long = ApiId(id).externalId.value

/** Convenience function to convert Long [ExternalId] value to a String [ApiId] value. */
fun externalIdToApiId(id: Long): String = ExternalId(id).apiId.value

/** Typesafe wrapper around Long to represent the integer id format used internally. */
data class InternalId(val value: Long) {
  init {
    require(value != 0L) { "0 is not permitted" }
  }
}

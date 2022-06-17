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

import java.net.URI
import java.net.URLDecoder

/** A [Map] of decoded key to value from the [URI.query]. */
val URI.queryMap: Map<String, String>
  get() =
    rawQuery.splitToSequence('&').associate {
      val parts: List<String> = it.split('=', limit = 2)
      val key = URLDecoder.decode(parts[0], Charsets.UTF_8.name())
      val value = if (parts.size == 2) URLDecoder.decode(parts[1], Charsets.UTF_8.name()) else ""
      key to value
    }

/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.grpc

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.wfanet.measurement.common.toJson

private val GSON = Gson()
private val SERVICE_CONFIG_MAP_TYPE = object : TypeToken<Map<String, *>>() {}.type

/** Immutable gRPC service configuration. */
sealed interface ServiceConfig {
  /** Returns the configuration as a JSON object map. */
  fun asMap(): Map<String, *>
}

/** [ServiceConfig] with JSON representation. */
data class JsonServiceConfig(val json: String) : ServiceConfig {
  override fun asMap(): Map<String, *> {
    return GSON.fromJson(json, SERVICE_CONFIG_MAP_TYPE)
  }
}

/** [ServiceConfig] with protobuf message representation. */
data class ProtobufServiceConfig(val message: io.grpc.serviceconfig.ServiceConfig) : ServiceConfig {
  fun asJson() = JsonServiceConfig(message.toJson())

  override fun asMap() = asJson().asMap()
}

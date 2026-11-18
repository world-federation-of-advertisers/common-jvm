// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.kms

import com.google.api.client.http.HttpRequestInitializer
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.auth.http.HttpTransportFactory
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Mockito

/** Loaded only by the Google factory test's service descriptor. */
class GoogleIdTokenTestTransportFactory : HttpTransportFactory {
  override fun create(): HttpTransport {
    val transport =
      object : MockHttpTransport() {
        override fun buildRequest(method: String, url: String): LowLevelHttpRequest =
          object : MockLowLevelHttpRequest(url) {
            override fun execute(): LowLevelHttpResponse = response(url, requests.incrementAndGet())
          }
      }
    // Stub Google authentication while exercising the SDK's token/error parsing and real AWS calls.
    val mock = Mockito.mock(HttpTransport::class.java)
    Mockito.doReturn(transport.createRequestFactory())
      .`when`(mock)
      .createRequestFactory(Mockito.any(HttpRequestInitializer::class.java))
    return mock
  }

  companion object {
    val requests = AtomicInteger()
    lateinit var response: (String, Int) -> LowLevelHttpResponse
  }
}

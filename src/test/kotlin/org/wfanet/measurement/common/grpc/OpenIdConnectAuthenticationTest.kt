/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import io.grpc.CallCredentials
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executor
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.mock
import org.wfanet.measurement.common.grpc.testing.OpenIdProvider
import org.wfanet.measurement.common.testing.verifyAndCapture

@RunWith(JUnit4::class)
class OpenIdConnectAuthenticationTest {
  @Test
  fun `verifyAndDecodeBearerToken returns VerifiedToken`() {
    val issuer = "example.com"
    val subject = "user1@example.com"
    val clientId = "foobar"
    val scopes = setOf("foo.bar", "foo.baz")
    val openIdProvider = OpenIdProvider(issuer, clientId)
    val credentials = openIdProvider.generateCredentials(subject, scopes)
    val auth = OpenIdConnectAuthentication(listOf(openIdProvider.providerConfig))

    val token = auth.verifyAndDecodeBearerToken(extractHeaders(credentials))

    assertThat(token).isEqualTo(OpenIdConnectAuthentication.VerifiedToken(issuer, subject, scopes))
  }

  @Test
  fun `verifyAndDecodeBearerToken throws UNAUTHENTICATED when token is expired`() {
    val issuer = "example.com"
    val subject = "user1@example.com"
    val clientId = "foobar"
    val scopes = setOf("foo.bar", "foo.baz")
    val openIdProvider = OpenIdProvider(issuer, clientId)
    val credentials =
      openIdProvider.generateCredentials(
        subject,
        scopes,
        Instant.now().minus(Duration.ofMinutes(5)),
      )
    val auth = OpenIdConnectAuthentication(listOf(openIdProvider.providerConfig))

    val exception =
      assertFailsWith<StatusException> {
        auth.verifyAndDecodeBearerToken(extractHeaders(credentials))
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("expired")
  }

  @Test
  fun `verifyAndDecodeBearerToken throws UNAUTHENTICATED when provider not found for issuer`() {
    val issuer = "example.com"
    val subject = "user1@example.com"
    val clientId = "foobar"
    val scopes = setOf("foo.bar", "foo.baz")
    val openIdProvider = OpenIdProvider(issuer, clientId)
    val credentials = openIdProvider.generateCredentials(subject, scopes)
    val auth = OpenIdConnectAuthentication(emptyList())

    val exception =
      assertFailsWith<StatusException> {
        auth.verifyAndDecodeBearerToken(extractHeaders(credentials))
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception).hasMessageThat().ignoringCase().contains("issuer")
  }

  @Test
  fun `verifyAndDecodeBearerToken throws UNAUTHENTICATED when token is not a valid JWT`() {
    val credentials = BearerTokenCallCredentials("foo", false)
    val auth = OpenIdConnectAuthentication(emptyList())

    val exception =
      assertFailsWith<StatusException> {
        auth.verifyAndDecodeBearerToken(extractHeaders(credentials))
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception).hasMessageThat().contains("JWT")
  }

  @Test
  fun `verifyAndDecodeBearerToken throws UNAUTHENTICATED when header not found`() {
    val auth = OpenIdConnectAuthentication(emptyList())

    val exception = assertFailsWith<StatusException> { auth.verifyAndDecodeBearerToken(Metadata()) }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception).hasMessageThat().contains("header")
  }

  private fun extractHeaders(credentials: BearerTokenCallCredentials): Metadata {
    val applierMock = mock<CallCredentials.MetadataApplier>()
    credentials.applyRequestMetadata(mock(), DirectExecutor, applierMock)
    return verifyAndCapture(applierMock, CallCredentials.MetadataApplier::apply)
  }

  private object DirectExecutor : Executor {
    override fun execute(command: Runnable) {
      command.run()
    }
  }
}

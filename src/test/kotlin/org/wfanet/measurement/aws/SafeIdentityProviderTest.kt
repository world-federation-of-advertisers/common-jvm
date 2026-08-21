// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.aws

import com.google.common.truth.Truth.assertThat
import java.security.GeneralSecurityException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import software.amazon.awssdk.identity.spi.IdentityProvider
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest

@RunWith(JUnit4::class)
class SafeIdentityProviderTest {

  @Test
  fun `resolveIdentity returns delegate result on success`() {
    val credentials = AwsSessionCredentials.create("access-key", "secret", "token")
    val delegate =
      object : IdentityProvider<AwsCredentialsIdentity> {
        override fun identityType(): Class<AwsCredentialsIdentity> =
          AwsCredentialsIdentity::class.java

        override fun resolveIdentity(
          request: ResolveIdentityRequest
        ): CompletableFuture<AwsCredentialsIdentity> =
          CompletableFuture.completedFuture(credentials)
      }
    val provider = SafeIdentityProvider(delegate)

    val resolved = provider.resolveIdentity(ResolveIdentityRequest.builder().build()).get()

    assertThat(resolved).isSameInstanceAs(credentials)
  }

  @Test
  fun `resolveIdentity converts a synchronous delegate failure into a failed future`() {
    val synchronousFailure = RuntimeException("token file not found")
    val delegate =
      object : IdentityProvider<AwsCredentialsIdentity> {
        override fun identityType(): Class<AwsCredentialsIdentity> =
          AwsCredentialsIdentity::class.java

        override fun resolveIdentity(
          request: ResolveIdentityRequest
        ): CompletableFuture<AwsCredentialsIdentity> = throw synchronousFailure
      }
    val provider = SafeIdentityProvider(delegate)

    val future = provider.resolveIdentity(ResolveIdentityRequest.builder().build())

    val exception = assertFailsWith<ExecutionException> { future.get() }
    assertThat(exception).hasCauseThat().isInstanceOf(GeneralSecurityException::class.java)
    assertThat(exception).hasCauseThat().hasCauseThat().isSameInstanceAs(synchronousFailure)
  }
}

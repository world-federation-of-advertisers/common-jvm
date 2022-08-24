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

package org.wfanet.measurement.aws.s3

import com.adobe.testing.s3mock.junit4.S3MockRule
import java.net.URI
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkAsyncHttpClientBuilder
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.utils.AttributeMap

private const val BUCKET = "test-bucket"

class S3StorageClientTest : AbstractStorageClientTest<S3StorageClient>() {
  @get:Rule val s3MockRule: S3MockRule = S3MockRule.builder().silent().build()

  @Before
  fun initClient() {
    val s3Client = s3MockRule.createAsyncClient()
    runBlocking { s3Client.createBucket { it.bucket(BUCKET) }.await() }

    storageClient = S3StorageClient(s3Client, BUCKET)
  }
}

/**
 * Creates an [S3AsyncClient].
 *
 * @see S3MockRule.createS3ClientV2
 */
private fun S3MockRule.createAsyncClient(): S3AsyncClient {
  return S3AsyncClient.builder()
    .region(Region.of("us-east-1"))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
    .httpClient(
      DefaultSdkAsyncHttpClientBuilder()
        .buildWithDefaults(
          AttributeMap.builder()
            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, java.lang.Boolean.TRUE)
            .build()
        )
    )
    .endpointOverride(URI.create(serviceEndpoint))
    .build()
}

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

package org.wfanet.measurement.gcloud.gcs

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import org.wfanet.measurement.common.Instrumentation
import picocli.CommandLine

/**
 * Client access provider for Google Cloud Storage (GCS) via command-line flags.
 *
 * @param flags command-line flags
 * @param retryConfig resilient retry/timeout configuration applied to the GCS client; defaults to
 *   [GcsStorageRetryConfig.DEFAULT]
 */
class GcsFromFlags(
  private val flags: Flags,
  private val retryConfig: GcsStorageRetryConfig = GcsStorageRetryConfig.DEFAULT,
) {

  private val storageOptions: StorageOptions by lazy {
    retryConfig.buildStorageOptions(
      projectId = flags.projectName,
      useGrpc = flags.useGrpc,
      openTelemetry = Instrumentation.openTelemetry,
    )
  }

  val storage: Storage
    get() = storageOptions.service

  val bucket: String
    get() = flags.bucket

  class Flags {
    @CommandLine.Option(
      names = ["--google-cloud-storage-project"],
      description = ["Name of the Google Cloud Storage project to use."],
      required = true,
    )
    lateinit var projectName: String
      private set

    @CommandLine.Option(
      names = ["--google-cloud-storage-bucket"],
      description = ["Name of the Google Cloud Storage project to use."],
      required = true,
    )
    lateinit var bucket: String
      private set

    @CommandLine.Option(
      names = ["--google-cloud-storage-use-grpc"],
      description = ["Whether to use gRPC instead of http."],
      required = false,
    )
    var useGrpc: Boolean = false
      private set
  }
}

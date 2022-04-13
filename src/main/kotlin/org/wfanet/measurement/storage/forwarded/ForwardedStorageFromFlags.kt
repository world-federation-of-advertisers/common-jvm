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

package org.wfanet.measurement.storage.forwarded

import java.io.File
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.grpc.buildTlsChannel
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/** Client access provider for Forwarded Storage via command-line flags. */
class ForwardedStorageFromFlags(flags: Flags) {
  val storageClient: StorageClient by lazy {
    val trustedCerts: Collection<X509Certificate> =
      readCertificateCollection(flags.trustedCertCollectionPem)
    ForwardedStorageClient(
      ForwardedStorageCoroutineStub(
        buildTlsChannel(
          flags.forwardedStorageServiceTarget,
          trustedCerts,
          flags.forwardedStorageCertHost
        )
      )
    )
  }

  class Flags {
    @CommandLine.Option(
      names = ["--forwarded-storage-service-target"],
      description = ["gRPC target (authority string or URI) for ForwardedStorage service."],
      required = true
    )
    lateinit var forwardedStorageServiceTarget: String
      private set

    @CommandLine.Option(
      names = ["--forwarded-storage-cert-host"],
      description = ["Expected hostname of ForwardedStorage service's TLS certificate."],
      required = false
    )
    var forwardedStorageCertHost: String? = null
      private set

    @CommandLine.Option(
      names = ["--trusted-cert-collection"],
      description = ["Collection of trusted X.509 certificates in PEM format"],
    )
    lateinit var trustedCertCollectionPem: File
      private set
  }
}

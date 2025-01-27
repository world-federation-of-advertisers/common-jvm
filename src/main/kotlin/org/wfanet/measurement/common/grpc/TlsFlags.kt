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

package org.wfanet.measurement.common.grpc

import java.io.File
import org.wfanet.measurement.common.crypto.SigningCerts
import picocli.CommandLine

class TlsFlags {
  @CommandLine.Option(
    names = ["--tls-cert-file"],
    description = ["User's own TLS cert file."],
    defaultValue = "",
  )
  lateinit var certFile: File
    private set

  @CommandLine.Option(
    names = ["--tls-key-file"],
    description = ["User's own TLS private key file."],
    defaultValue = "",
  )
  lateinit var privateKeyFile: File
    private set

  @CommandLine.Option(
    names = ["--cert-collection-file"],
    description = ["Trusted root Cert collection file."],
    required = false,
  )
  var certCollectionFile: File? = null
    private set

  val signingCerts: SigningCerts by lazy {
    SigningCerts.fromPemFiles(certFile, privateKeyFile, certCollectionFile)
  }
}

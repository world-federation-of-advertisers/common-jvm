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

package org.wfanet.measurement.common.crypto.tools

import java.io.File
import java.security.cert.X509Certificate
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyStore
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.ParentCommand

/** Command-line utility for private key storage. */
@Command(
  description = ["Utility for private key storage."],
  subcommands = [CommandLine.HelpCommand::class, SigningKeys::class]
)
abstract class KeyStoreUtil {
  abstract val storageClient: StorageClient
}

@Command(
  name = "signing-keys",
  description = ["Subcommand for signing key storage."],
  subcommands = [CommandLine.HelpCommand::class]
)
class SigningKeys {
  @ParentCommand private lateinit var keyStoreUtil: KeyStoreUtil

  val keyStore: SigningKeyStore by lazy { SigningKeyStore(keyStoreUtil.storageClient) }

  @Command(
    name = "write",
    description = ["Writes a signing key to storage.", "Prints the blob key to STDOUT."],
  )
  fun write(
    @Option(
      names = ["--certificate"],
      description = ["Path to X.509 certificate in PEM or DER format"],
      required = true,
    )
    certificateFile: File,
    @Option(
      names = ["--private-key"],
      description = ["Path to private key in PEM format"],
      required = true,
    )
    privateKeyFile: File,
    @Option(
      names = ["--quiet"],
      description = ["Whether to suppress output of the blob key"],
      defaultValue = "false"
    )
    quiet: Boolean
  ) {
    val certificate: X509Certificate = readCertificate(certificateFile)
    val signingKey =
      SigningKeyHandle(certificate, readPrivateKey(privateKeyFile, certificate.publicKey.algorithm))
    val blobKey = runBlocking { signingKey.write(keyStore) }

    if (!quiet) {
      println(blobKey)
    }
  }
}

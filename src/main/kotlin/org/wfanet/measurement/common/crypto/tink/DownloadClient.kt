// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.storage


import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.crypto.SignedBlob
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient.VerifiedBlob
import org.wfanet.panelmatch.protocol.NamedSignature

/**
 * Interface for that downloads blob from shared storage.
 */
interface DownloadClient {

  /**
   * Reads [blobKey] from shared storage. Returns a [VerifiedBlob] which checks the blob's signature
   * using [x509] when it is read.
   *
   * Note that the final signature validation does not happen until the [Flow] underlying the
   * [VerifiedBlob] is collected.
   */
  @Throws(BlobNotFoundException::class)
  suspend fun getBlob(sharedStorage: StorageClient, blobKey: String, x509: X509Certificate): VerifiedBlob

  suspend fun verifyBlob(
    signedBlob: SignedBlob,
    namedSignature: NamedSignature,
    x509: X509Certificate
  ): VerifiedBlob
}

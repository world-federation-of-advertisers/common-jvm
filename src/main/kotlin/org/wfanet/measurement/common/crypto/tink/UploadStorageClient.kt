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

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.protocol.NamedSignature
import java.io.Serializable
import org.wfanet.measurement.common.crypto.SignedBlob


/**
 * Interface for [StorageClient] that writes (blob, signature) pairs to shared storage.
 */
interface UploadStorageClient {

  /**
   * Writes [content] to shared storage using the given [blobKey]. Also writes a serialized
   * [NamedSignature] for the blob.
   */
  suspend fun writeBlob(
    sharedStorage: StorageClient,
    blobKey: String,
    content: Flow<ByteString>,
    x509: X509Certificate,
    privateKey: PrivateKey
  ): Serializable

  suspend fun signBlob(
    content: Flow<ByteString>,
    x509: X509Certificate,
    privateKey: PrivateKey
  ): SignedBlob

}

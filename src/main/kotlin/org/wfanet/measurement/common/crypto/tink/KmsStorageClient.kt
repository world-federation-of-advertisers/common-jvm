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

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.Aead
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient

internal class KmsStorageClient(private val storageClient: StorageClient, private val aead: Aead) :
  StorageClient {

  override val defaultBufferSizeBytes: Int
    get() = storageClient.defaultBufferSizeBytes

  override suspend fun createBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    TODO("Not yet implemented")
  }

  override fun getBlob(blobKey: String): StorageClient.Blob? {
    TODO("Not yet implemented")
  }
}

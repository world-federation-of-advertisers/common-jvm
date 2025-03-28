// Copyright 2025 The Cross-Media Measurement Authors
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

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.TinkJsonProtoKeysetFormat
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.storage.StorageClient

data class WrappedStorageConfig(val kekUri: String, val encryptedDek: String, val aesMode: String)

class WrappedKeysStorageClient(
  private val storageClient: StorageClient,
  private val kmsClient: KmsClient,
  private val config: WrappedStorageConfig,
  private val aeadContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : StorageClient {

  private val streamingAeadStorageClient by lazy {
    runBlocking {
      val kekAead = kmsClient.getAead(config.kekUri)
      val handle =
        withContext(aeadContext) {
          TinkJsonProtoKeysetFormat.parseEncryptedKeyset(
            config.encryptedDek,
            kekAead,
            byteArrayOf(),
          )
        }
      when (config.aesMode) {
        "AES128_GCM_HKDF_1MB" -> {

          StreamingAeadConfig.register()
          StreamingAeadStorageClient(
            storageClient = storageClient,
            streamingAead = handle.getPrimitive(StreamingAead::class.java),
            streamingAeadContext = aeadContext,
          )
        }
        else -> throw IllegalArgumentException("Unsupported AES Mode ${config.aesMode}")
      }
    }
  }

  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {
    return streamingAeadStorageClient.writeBlob(blobKey, content)
  }

  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    return streamingAeadStorageClient.getBlob(blobKey)
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

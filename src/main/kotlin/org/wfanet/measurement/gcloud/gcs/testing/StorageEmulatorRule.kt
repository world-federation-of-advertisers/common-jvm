/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.gcloud.gcs.testing

import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import java.util.logging.Level
import java.util.logging.Logger
import org.jetbrains.annotations.Blocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

class StorageEmulatorRule : TestRule {
  lateinit var storage: Storage

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        StorageEmulator().use { emulator ->
          storage = emulator.start()
          emulator.onExit().exceptionally { e: Throwable ->
            logger.log(Level.SEVERE, e) { "GCS emulator exited exceptionally" }
          }
          storage.use { base.evaluate() }
        }
      }
    }
  }

  @Blocking
  fun createBucket(bucket: String) {
    storage.create(BucketInfo.of(bucket))
  }

  @Blocking
  fun deleteBucketRecursive(bucket: String) {
    // Batch delete is not supported by storage-testbench, so delete each blob one-by-one rather
    // than batching by page.
    storage.list(bucket).iterateAll().forEach { it.delete() }

    storage.delete(bucket)
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.enclosingClass.name)
  }
}

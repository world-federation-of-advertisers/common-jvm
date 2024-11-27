// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.pubsub.testing

import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName

/** [GooglePubSubEmulatorProvider] implementation as a JUnit [TestRule]. */
class GooglePubSubEmulatorProvider : TestRule {

  private lateinit var pubsubEmulator: PubSubEmulatorContainer

  val host: String
    get() = pubsubEmulator.host

  val port: Int
    get() = pubsubEmulator.getMappedPort(8085)

  fun startEmulator() {
    pubsubEmulator = PubSubEmulatorContainer(DockerImageName.parse(PUBSUB_IMAGE_NAME))
    pubsubEmulator.start()
  }

  fun stopEmulator() {
    if (::pubsubEmulator.isInitialized) {
      pubsubEmulator.stop()
    }
  }

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        try {
          startEmulator()
          base.evaluate()
        } finally {
          stopEmulator()
        }
      }
    }
  }

  companion object {
    const val PUBSUB_IMAGE_NAME = "gcr.io/google.com/cloudsdktool/cloud-sdk:317.0.0-emulators"
  }
}

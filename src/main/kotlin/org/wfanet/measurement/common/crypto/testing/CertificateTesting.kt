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

package org.wfanet.measurement.common.crypto.testing

import java.io.File
import java.nio.file.Paths
import org.wfanet.measurement.common.getRuntimePath

private val TESTDATA_DIR_PATH =
  Paths.get(
    "wfa_common_jvm",
    "src",
    "main",
    "kotlin",
    "org",
    "wfanet",
    "measurement",
    "common",
    "crypto",
    "testing",
    "testdata"
  )

const val KEY_ALGORITHM = "EC"

/**
 * For some tests, we used a fixed certificate server.*. All the other certificates are generated
 * dynamically each test.
 */
val FIXED_SERVER_CERT_PEM_FILE = getRuntimePath(TESTDATA_DIR_PATH.resolve("server.pem"))!!.toFile()
val FIXED_SERVER_KEY_FILE = getRuntimePath(TESTDATA_DIR_PATH.resolve("server.key"))!!.toFile()
val FIXED_CA_CERT_PEM_FILE = getRuntimePath(TESTDATA_DIR_PATH.resolve("ca.pem"))!!.toFile()

private const val TESTDATA_DIR =
  "src/main/kotlin/org/wfanet/measurement/common/crypto/testing/testdata/"

val EDP_1_CERT_PEM_FILE = File("${TESTDATA_DIR}edp_1_server.pem")
val EDP_1_KEY_FILE = File("${TESTDATA_DIR}edp_1_server.key")

val DUCHY_1_NON_AGG_CERT_PEM_FILE = File("${TESTDATA_DIR}non_aggregator_1_server.pem")
val DUCHY_1_NON_AGG_KEY_FILE = File("${TESTDATA_DIR}non_aggregator_1_server.key")

val DUCHY_AGG_CERT_PEM_FILE = File("${TESTDATA_DIR}aggregator_server.pem")
val DUCHY_AGG_KEY_FILE = File("${TESTDATA_DIR}aggregator_server.key")

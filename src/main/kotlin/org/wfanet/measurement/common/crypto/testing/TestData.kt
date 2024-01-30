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

/** Test data files. */
object TestData {
  /**
   * For some tests, we used a fixed certificate server.*. All the other certificates are generated
   * by bazel and are likely cached by bazel.
   */
  private val FIXED_TESTDATA_DIR: File =
    getRuntimePath(
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
      )!!
      .toFile()

  val FIXED_NO_AKID_CERT_PEM_FILE = FIXED_TESTDATA_DIR.resolve("no_akid.pem")
  val FIXED_SERVER_CERT_PEM_FILE = FIXED_TESTDATA_DIR.resolve("server.pem")
  val FIXED_SERVER_KEY_FILE = FIXED_TESTDATA_DIR.resolve("server.key")
  val FIXED_SERVER_CERT_DER_FILE = FIXED_TESTDATA_DIR.resolve("server-cert.der")
  val FIXED_SERVER_KEY_DER_FILE = FIXED_TESTDATA_DIR.resolve("server-key.der")
  val FIXED_CA_CERT_PEM_FILE = FIXED_TESTDATA_DIR.resolve("ca.pem")
  val FIXED_CA_KEY_FILE = FIXED_TESTDATA_DIR.resolve("ca.key")
  val FIXED_CLIENT_CERT_PEM_FILE = FIXED_TESTDATA_DIR.resolve("client-cert.pem")
  val FIXED_EXPIRED_CERT_PEM_FILE = FIXED_TESTDATA_DIR.resolve("expired.pem")
  val FIXED_EXPIRED_CERT_KEY_FILE = FIXED_TESTDATA_DIR.resolve("expired.key")

  /** PEM file containing a fixed CA certificate with an RSA key and an RSASSA-PSS signature. */
  val FIXED_CA_CERT_WITH_RSA_PSS_SIG_PEM_FILE = FIXED_TESTDATA_DIR.resolve("ca-rsa.pem")
  /**
   * PEM file containing a fixed certificate with an EC key and an RSASSA-PSS signature.
   *
   * Issued by [FIXED_CA_CERT_WITH_RSA_PSS_SIG_PEM_FILE].
   */
  val FIXED_CERT_WITH_RSA_PSS_SIG_PEM_FILE = FIXED_TESTDATA_DIR.resolve("user-ec-rsa-sig.pem")
  /** Private key file for [FIXED_CERT_WITH_RSA_PSS_SIG_PEM_FILE] in PEM format. */
  val FIXED_CERT_WITH_RSA_PSS_SIG_KEY_FILE = FIXED_TESTDATA_DIR.resolve("user-ec-rsa-sig.key")

  val FIXED_ENCRYPTION_PRIVATE_KEYSET = FIXED_TESTDATA_DIR.resolve("enc-private.tink")
  val FIXED_ENCRYPTION_PUBLIC_KEYSET = FIXED_TESTDATA_DIR.resolve("enc-public.tink")
}

@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_SERVER_CERT_PEM_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_SERVER_CERT_PEM_FILE = TestData.FIXED_SERVER_CERT_PEM_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_SERVER_KEY_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_SERVER_KEY_FILE = TestData.FIXED_SERVER_KEY_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_SERVER_CERT_DER_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_SERVER_CERT_DER_FILE = TestData.FIXED_SERVER_CERT_DER_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_SERVER_KEY_DER_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_SERVER_KEY_DER_FILE = TestData.FIXED_SERVER_KEY_DER_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_CA_CERT_PEM_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_CA_CERT_PEM_FILE = TestData.FIXED_CA_CERT_PEM_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith("TestData.FIXED_CA_KEY_FILE", "org.wfanet.measurement.common.crypto.testing.TestData")
)
val FIXED_CA_KEY_FILE = TestData.FIXED_CA_KEY_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_CLIENT_CERT_PEM_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_CLIENT_CERT_PEM_FILE = TestData.FIXED_CLIENT_CERT_PEM_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_EXPIRED_CERT_PEM_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_EXPIRED_CERT_PEM_FILE = TestData.FIXED_EXPIRED_CERT_PEM_FILE
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_EXPIRED_CERT_KEY_FILE",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_EXPIRED_CERT_KEY_FILE = TestData.FIXED_EXPIRED_CERT_KEY_FILE

@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_ENCRYPTION_PRIVATE_KEYSET",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_ENCRYPTION_PRIVATE_KEYSET = TestData.FIXED_ENCRYPTION_PRIVATE_KEYSET
@Deprecated(
  "Use TestData",
  ReplaceWith(
    "TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET",
    "org.wfanet.measurement.common.crypto.testing.TestData"
  )
)
val FIXED_ENCRYPTION_PUBLIC_KEYSET = TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET

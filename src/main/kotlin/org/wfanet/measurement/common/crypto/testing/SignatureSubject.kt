/*
 * Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.crypto.testing

import com.google.common.truth.Fact
import com.google.common.truth.FailureMetadata
import com.google.common.truth.Subject
import com.google.common.truth.Truth
import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.verifySignature

class SignatureSubject private constructor(failureMetadata: FailureMetadata, subject: ByteString) :
  Subject(failureMetadata, subject) {

  private val actual = subject

  fun isValidFor(certificate: X509Certificate, data: ByteString) {
    if (!certificate.verifySignature(data, actual)) {
      failWithActual(
        Fact.simpleFact("cannot be verified"),
        Fact.fact("certificate SKID", certificate.subjectKeyIdentifier),
        Fact.fact("data", data)
      )
    }
  }

  companion object {
    fun assertThat(actual: ByteString): SignatureSubject =
      Truth.assertAbout(signatures()).that(actual)

    fun signatures(): (failureMetadata: FailureMetadata, subject: ByteString) -> SignatureSubject =
      ::SignatureSubject
  }
}

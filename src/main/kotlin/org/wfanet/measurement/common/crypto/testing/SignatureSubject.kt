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

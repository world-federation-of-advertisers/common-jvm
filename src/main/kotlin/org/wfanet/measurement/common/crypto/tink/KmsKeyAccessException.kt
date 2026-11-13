// Copyright 2026 The Cross-Media Measurement Authors
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

import java.security.GeneralSecurityException

/**
 * KMS client or key-access failure with a recommendation for bounded retries.
 *
 * [isRetryable] describes the adapter's recommendation, not a guarantee that another attempt
 * succeeds. Opaque cryptographic failures have no retry information and default to false. Existing
 * callers can continue catching [GeneralSecurityException].
 */
class KmsKeyAccessException(message: String, val isRetryable: Boolean, cause: Throwable? = null) :
  GeneralSecurityException(message, cause)

#!/bin/bash
#
# Copyright 2021 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -o pipefail

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
}

# Check that user certificate is signed by root CA
openssl verify -x509_strict -verbose -CAfile "${2}" "${4}"
if [[ $? -ne 0 ]]; then
  err "Unable to validate user certificate is signed by root CA"
  exit 1
fi

# Check that private key is proper EC key
openssl ec -check -noout -in "${3}"
if [[ $? -ne 0 ]]; then
  err "Unable to check that private key is proper EC key"
  exit 1
fi

# Check that public keys match
if [[ "$(openssl pkey -pubout -in "${3}")" != "$(openssl x509 -noout -pubkey -in "${4}")" ]]; then
  err "Public keys do not match"
  exit 1
fi

# Check the certificate details
certificate_details=$(openssl x509 -noout -subject -in "${4}" 2>&1)
if [[ "$certificate_details" != $'subject=O = Some User Org, CN = some-user.com' ]]; then
  err "Invalid Certificate Details: ${certificate_details}"
  exit 1
fi

echo "Success"
exit 0

#!/usr/bin/env bash
#
# Validate that root certificates were generated correctly
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

main() {
  local -r root_key_file="$1"
  local -r root_certificate_pem_file="$2"

  # Check that certificate verifies itself when it is set as the CA. Note that because we include the SAN extension in
  # openssl.cnf, `-verify_hostname` checks for localhost.
  if ! openssl verify -x509_strict -verbose -CAfile "${root_certificate_pem_file}" -verify_hostname localhost "${root_certificate_pem_file}"; then
    err 'Unable to validate hostname'
    exit 1
  fi

  # Check that private key is proper EC key
  if ! openssl ec -check -noout -in "${root_key_file}"; then
    err 'Unable to check that private key is proper EC key'
    exit 1
  fi

  # Check that public keys match
  if [[ "$(openssl pkey -pubout -in "${root_key_file}" 2>&1)" != "$(openssl x509 -noout -pubkey -in "${root_certificate_pem_file}" 2>&1)" ]]; then
    err 'Public keys do not match'
    exit 1
  fi

  # Check the certificate details
  certificate_details=$(openssl x509 -noout -subject -in "${root_certificate_pem_file}" 2>&1)
  if [[ "${certificate_details}" != $'subject=O = Some Root OrgCA, CN = some-ca.com' ]]; then
    err "Invalid Certificate Details: ${certificate_details}"
    exit 1
  fi

  err 'Success'
}

main "$@"

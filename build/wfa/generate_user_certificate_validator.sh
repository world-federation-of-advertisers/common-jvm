#!/usr/bin/env bash
#
# Validate that user certificates were generated correctly
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
  local -r user_key_file="$3"
  local -r user_certificate_pem_file="$4"

  # Check that user certificate is signed by root CA and has correct hostname
  if ! openssl verify -x509_strict -verbose -CAfile "${root_certificate_pem_file}" -verify_hostname 'some-user.com' "${user_certificate_pem_file}"; then
    err 'Unable to validate user certificate is signed by root CA with correct hostname'
    exit 1
  fi

  # Check that private key is proper EC key
  if ! openssl ec -check -noout -in "${user_key_file}"; then
    err 'Unable to check that private key is proper EC key'
    exit 1
  fi

  # Check that public keys match
  if [[ "$(openssl pkey -pubout -in "${user_key_file}")" != "$(openssl x509 -noout -pubkey -in "${user_certificate_pem_file}")" ]]; then
    err 'Public keys do not match'
    exit 1
  fi

  # Check the certificate details
  certificate_details=$(openssl x509 -noout -subject -in "${user_certificate_pem_file}" 2>&1)
  if [[ "$certificate_details" != $'subject=O = Some User Org, CN = some-user.com' ]]; then
    err "Invalid Certificate Details: ${certificate_details}"
    exit 1
  fi

  err 'Success'
}

main "$@"

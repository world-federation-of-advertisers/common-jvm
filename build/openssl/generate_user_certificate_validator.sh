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

# --- begin runfiles.bash initialization v2 ---
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
 source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
 source "$0.runfiles/$f" 2>/dev/null || \
 source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
 source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
 { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

set -eEu -o pipefail

readonly WORKSPACE='wfa_common_jvm'
readonly TESTDATA_DIR="${WORKSPACE}/build/openssl"
readonly ROOT_CERT_PATH="${TESTDATA_DIR}/test_root.pem"
readonly ROOT_KEY_PATH="${TESTDATA_DIR}/test_root.key"
readonly CERT_PATH="${TESTDATA_DIR}/test_user.pem"
readonly KEY_PATH="${TESTDATA_DIR}/test_user.key"
readonly ORG='Some Organization'
readonly COMMON_NAME='Some Server'
readonly SUBJECT_HASH='bf3afa36'
readonly HOSTNAME='server.someorg.example.com'

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
}

main() {
  local root_cert_file
  root_cert_file="$(rlocation "${ROOT_CERT_PATH}")"
  local root_key_file
  root_key_file="$(rlocation "${ROOT_KEY_PATH}")"
  local cert_file
  cert_file="$(rlocation "${CERT_PATH}")"
  local key_file
  key_file="$(rlocation "${KEY_PATH}")"

  # Check that user certificate is signed by root CA.
  openssl verify -x509_strict -verbose -CAfile "${root_cert_file}" -verify_hostname "${HOSTNAME}" "${cert_file}"

  # Check that private key is proper EC key
  openssl ec -check -noout -in "${key_file}"

  # Check that public keys match
  if [[ "$(openssl pkey -pubout -in "${key_file}")" != "$(openssl x509 -noout -pubkey -in "${cert_file}")" ]]; then
    err 'Public keys do not match'
    exit 1
  fi

  # Check the certificate details
  local subject_hash
  subject_hash="$(openssl x509 -in "${cert_file}" -noout -subject_hash)"
  if [[ "${subject_hash}" != "${SUBJECT_HASH}" ]]; then
    err "Not true that subject hash ${subject_hash} is equal to ${SUBJECT_HASH}"
    exit 1
  fi
}

main "$@"

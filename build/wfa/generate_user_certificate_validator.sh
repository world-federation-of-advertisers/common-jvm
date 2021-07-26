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

set -ex

# Check that user certificate is signed by root CA
certificate_chain_check=$(openssl verify -x509_strict -verbose -CAfile $2 $4)
if [ "$certificate_chain_check" != $'build/wfa/test_user.pem: OK' ]; then
  echo "Invalid Certificate Chain:"
  echo $certificate_chain_check
  exit 1
fi

# Check that private key is proper EC key
ec_key_check=$(openssl ec -check -noout -in $3 2>&1)
if [ "$ec_key_check" != $'read EC key\nEC Key valid.' ]; then
  echo "Invalid EC Key:"
  echo $ec_key_check
  exit 1
fi

# Check that public keys match
if [ "$(openssl pkey -pubout -in $3)" != "$(openssl x509 -noout -pubkey -in $4)" ]; then
  echo "Public keys do not match"
  exit 1
fi

# Check the certificate details
certificate_details=$(openssl x509 -noout -subject -in  $4 2>&1)
if [ "$certificate_details" != $'subject=O = Some User Org, CN = some-user.com' ]; then
  echo "Invalid Certificate Details:"
  echo $certificate_details
  exit 1
fi

echo "Success"
exit 0

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

"""Repository targets for Tink (https://github.com/google/tink)."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

_COMMIT = "9753ffddd4d04aa56e0605ff4a0db46f2fb80529"  # HEAD on 2021-11-30.
_SHA256 = "8399247fc7aec54062f757ce83e07f2ed2e80e8d1765a8e9eab4027b228c1c9e"
_URL = "https://github.com/google/tink/archive/{commit}.tar.gz".format(commit = _COMMIT)

# Dict of Maven artifacts for Tink Java KMS integration.
TINK_JAVA_KMS_MAVEN_ARTIFACTS = {
    # Auto Service.
    "com.google.auto:auto-common": "0.10",
    "com.google.auto.service:auto-service": "1.0-rc7",
    "com.google.auto.service:auto-service-annotations": "1.0-rc7",

    # Google Cloud KMS.
    "com.google.apis:google-api-services-cloudkms": "v1-rev108-1.25.0",

    # AWS KMS.
    "com.amazonaws:aws-java-sdk-core": "1.11.976",
    "com.amazonaws:aws-java-sdk-kms": "1.11.976",
}

def tink_java():
    _tink_base()

    if "tink_java" not in native.existing_rules():
        http_archive(
            name = "tink_java",
            url = _URL,
            sha256 = _SHA256,
            strip_prefix = "tink-{commit}/java_src".format(commit = _COMMIT),
        )

def _tink_base():
    if "tink_base" not in native.existing_rules():
        http_archive(
            name = "tink_base",
            url = _URL,
            sha256 = _SHA256,
            strip_prefix = "tink-{commit}".format(commit = _COMMIT),
        )

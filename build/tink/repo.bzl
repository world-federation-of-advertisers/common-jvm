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
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("//build:versions.bzl", "TINK_COMMIT")

_SHA256 = "0b8bbaffee4903faea66dbad76f8eb6d0eea3f94367807bebc49180f9f417031"
_URL = "https://github.com/google/tink/archive/{commit}.tar.gz".format(commit = TINK_COMMIT)

# Dict of Maven artifacts for Tink Java KMS integration.
TINK_JAVA_KMS_MAVEN_DEPS = {
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

    # TODO(@SanjayVas): Depend on Maven artifact instead once everything
    # we use from Tink is included in a Maven release.
    maybe(
        http_archive,
        name = "tink_java",
        url = _URL,
        sha256 = _SHA256,
        strip_prefix = "tink-{commit}/java_src".format(commit = TINK_COMMIT),
    )

def tink_cc():
    _tink_base()

    maybe(
        http_archive,
        name = "tink_cc",
        url = _URL,
        sha256 = _SHA256,
        strip_prefix = "tink-{commit}/cc".format(commit = TINK_COMMIT),
        repo_mapping = {
            # TODO(bazelbuild/rules_proto#121): Remove this once
            # protobuf_workspace is fixed.
            "@com_google_protobuf": "@com_github_protocolbuffers_protobuf",
        },
    )
    _tink_base()


def _tink_base():
    maybe(
        http_archive,
        name = "tink_base",
        url = _URL,
        sha256 = _SHA256,
        strip_prefix = "tink-{commit}".format(commit = TINK_COMMIT),
    )

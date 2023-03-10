# Copyright 2022 The Cross-Media Measurement Authors
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

"""Repository rules/macros for rules_proto."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("//build:versions.bzl", "PROTOBUF_JAVA_VERSION")

_MAVEN_COORDINATES = [
    "com.google.protobuf:protobuf-java",
    "com.google.protobuf:protobuf-java-util",
]

EXCLUDED_ARTIFACTS = [
    # protobuf-java and protobuf-javalite cannot coexist.
    "com.google.protobuf:protobuf-javalite",
]

def rules_proto():
    maybe(
        http_archive,
        name = "rules_proto",
        sha256 = "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
        strip_prefix = "rules_proto-5.3.0-21.7",
        urls = [
            "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
        ],
    )

def rules_proto_maven_artifacts_dict():
    return {coordinates: PROTOBUF_JAVA_VERSION for coordinates in _MAVEN_COORDINATES}

# Copyright 2020 The Cross-Media Measurement Authors
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

"""Repository rules/macros for rules_kotlin.

See https://github.com/bazelbuild/rules_kotlin
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("//build:versions.bzl", "KOTLIN_RELEASE_VERSION")

_JETBRAINS_KOTLIN_LIBRARIES = [
    "org.jetbrains.kotlin:kotlin-stdlib",
    "org.jetbrains.kotlin:kotlin-stdlib-common",
    "org.jetbrains.kotlin:kotlin-reflect",
    "org.jetbrains.kotlin:kotlin-test",
]

def _rules_kotlin_repo(version, sha256):
    maybe(
        http_archive,
        name = "io_bazel_rules_kotlin",
        urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/%s/rules_kotlin_release.tgz" % version],
        sha256 = sha256,
    )

def io_bazel_rules_kotlin():
    _rules_kotlin_repo(
        version = "v1.8-RC-12",
        sha256 = "8e5c8ab087e0fa3fbb58e1f6b99d8fe40f75bac44994c3d208eba723284465d6",
    )

def rules_kotlin_maven_artifacts_dict():
    artifacts_dict = {
        coordinates: KOTLIN_RELEASE_VERSION
        for coordinates in _JETBRAINS_KOTLIN_LIBRARIES
    }
    return artifacts_dict

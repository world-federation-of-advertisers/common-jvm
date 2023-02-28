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

_JETBRAINS_KOTLIN_OVERRIDE_TARGETS = {
    "org.jetbrains.kotlin:kotlin-stdlib": "@com_github_jetbrains_kotlin//:kotlin-stdlib",
    "org.jetbrains.kotlin:kotlin-stdlib-common": "@com_github_jetbrains_kotlin//:kotlin-stdlib",
    "org.jetbrains.kotlin:kotlin-stdlib-jdk7": "@com_github_jetbrains_kotlin//:kotlin-stdlib-jdk7",
    "org.jetbrains.kotlin:kotlin-stdlib-jdk8": "@com_github_jetbrains_kotlin//:kotlin-stdlib-jdk8",
    "org.jetbrains.kotlin:kotlin-reflect": "@com_github_jetbrains_kotlin//:kotlin-reflect",
    "org.jetbrains.kotlin:kotlin-test": "@com_github_jetbrains_kotlin//:kotlin-test",
}

_JETBRAINS_OVERRIDE_TARGETS = {
    "org.jetbrains:annotations": "@com_github_jetbrains_kotlin//:annotations",
}  # @unused

# Override targets for rules_kotlin.
#
# Despite the fact that the Kotlin compiler release bundles JetBrains,
# annotations, we intentionally do not include it as an override target since
# as of the 1.6 compiler release it is quite an old version (13).
RULES_KOTLIN_OVERRIDE_TARGETS = _JETBRAINS_KOTLIN_OVERRIDE_TARGETS

def _rules_kotlin_repo(version, sha256):
    maybe(
        http_archive,
        name = "io_bazel_rules_kotlin",
        urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/%s/rules_kotlin_release.tgz" % version],
        sha256 = sha256,
    )

def io_bazel_rules_kotlin():
    _rules_kotlin_repo(
        version = "v1.6.0-RC-2",
        sha256 = "88d19c92a1fb63fb64ddb278cd867349c3b0d648b6fe9ef9a200b9abcacd489d",
    )

def rules_kotlin_maven_artifacts_dict():
    artifacts_dict = {
        coordinates: KOTLIN_RELEASE_VERSION
        for coordinates in _JETBRAINS_KOTLIN_OVERRIDE_TARGETS.keys()
    }
    return artifacts_dict

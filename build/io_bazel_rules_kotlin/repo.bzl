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

IO_BAZEL_RULES_KOTLIN_OVERRIDE_TARGETS = {
    "org.jetbrains.kotlin:kotlin.stdlib": "@com_github_jetbrains_kotlin//:kotlin-stdlib",
    "org.jetbrains.kotlin:kotlin-stdlib-common": "@com_github_jetbrains_kotlin//:kotlin-stdlib",
    "org.jetbrains.kotlin:kotlin.stdlib-jdk7": "@com_github_jetbrains_kotlin//:kotlin-stdlib-jdk7",
    "org.jetbrains.kotlin:kotlin.stdlib-jdk8": "@com_github_jetbrains_kotlin//:kotlin-stdlib-jdk8",
    "org.jetbrains.kotlin:kotlin.reflect": "@com_github_jetbrains_kotlin//:kotlin-reflect",
    "org.jetbrains.kotlin:kotlin.test": "@com_github_jetbrains_kotlin//:kotlin-test",
}

def _rules_kotlin_repo(version, sha256):
    http_archive(
        name = "io_bazel_rules_kotlin",
        urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/%s/rules_kotlin_release.tgz" % version],
        sha256 = sha256,
    )

def rules_kotlin_repo():
    _rules_kotlin_repo(
        version = "v1.5.0",
        sha256 = "12d22a3d9cbcf00f2e2d8f0683ba87d3823cb8c7f6837568dd7e48846e023307",
    )

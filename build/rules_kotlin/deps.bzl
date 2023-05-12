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

"""Repository rules/macros for rules_kotlin dependencies."""

load(
    "@io_bazel_rules_kotlin//kotlin:repositories.bzl",
    "kotlin_repositories",
    "kotlinc_version",
)
load("//build:versions.bzl", "KOTLIN_RELEASE_VERSION")

# Version of org.jetbrains:annotations that comes bundled with
# KOTLIN_RELEASE_VERSION.
JETBRAINS_ANNOTATIONS_VERSION = "13.0"

# buildifier: disable=unnamed-macro
def rules_kotlin_deps():
    compiler_release = kotlinc_version(
        release = KOTLIN_RELEASE_VERSION,
        sha256 = "6e43c5569ad067492d04d92c28cdf8095673699d81ce460bd7270443297e8fd7",
    )
    kotlin_repositories(
        compiler_release = compiler_release,
    )
    native.register_toolchains(
        "@wfa_common_jvm//build/rules_kotlin/toolchain:toolchain",
    )

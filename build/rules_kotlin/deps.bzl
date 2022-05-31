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

def rules_kotlin_deps():
    kotlin_repositories(
        compiler_release = kotlinc_version(
            release = KOTLIN_RELEASE_VERSION,
            sha256 = "632166fed89f3f430482f5aa07f2e20b923b72ef688c8f5a7df3aa1502c6d8ba",
        ),
    )

    native.register_toolchains(
        "@wfa_common_jvm//build/rules_kotlin/toolchain:toolchain",
    )

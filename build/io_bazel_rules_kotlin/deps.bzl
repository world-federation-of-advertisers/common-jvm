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

load("@io_bazel_rules_kotlin//kotlin:repositories.bzl", "kotlin_repositories", "kotlinc_version")
load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kt_register_toolchains")

def rules_kotlin_deps():
    kotlin_repositories(
        compiler_release = kotlinc_version(
            release = "1.3.31", # just the numeric version
            sha256 = "107325d56315af4f59ff28db6837d03c2660088e3efeb7d4e41f3e01bb848d6a"
        )
    )

    kt_register_toolchains()

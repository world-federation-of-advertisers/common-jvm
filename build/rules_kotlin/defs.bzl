# Copyright 2023 The Cross-Media Measurement Authors
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

"""Macros for kt_jvm_library to include Maven kotlin stdlib."""

load("@io_bazel_rules_kotlin//kotlin:jvm.bzl", "kt_jvm_library")

def kt_jvm_library_with_maven_kotlin_stdlib(name, deps = [], **kwargs):
    kt_jvm_library(
        name = name,
        deps = [
            "@maven//:org_jetbrains_kotlin_kotlin_stdlib",
            "@maven//:org_jetbrains_kotlin_kotlin_reflect",
        ] + deps,
        **kwargs
    )

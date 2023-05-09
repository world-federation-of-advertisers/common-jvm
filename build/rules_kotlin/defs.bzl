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

load("@io_bazel_rules_kotlin//kotlin:jvm.bzl", old_kt_jvm_library = "kt_jvm_library")

def kt_jvm_library(name, deps = [], **kwargs):
    old_kt_jvm_library(
        name = name,
        deps = [
            "//imports/kotlin/kotlin:stdlib",
            "//imports/kotlin/kotlin/reflect",
        ] + deps,
        **kwargs
    )

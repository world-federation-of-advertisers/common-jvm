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

load(
    "//build:versions.bzl",
    "KOTLIN_RELEASE_VERSION",
    "RULES_KOTLIN",
    "versioned_http_archive",
)

_JETBRAINS_KOTLIN_LIBRARIES = [
    "org.jetbrains.kotlin:kotlin-stdlib",
    "org.jetbrains.kotlin:kotlin-stdlib-common",
    "org.jetbrains.kotlin:kotlin-reflect",
    "org.jetbrains.kotlin:kotlin-test",
]

def io_bazel_rules_kotlin():
    versioned_http_archive(RULES_KOTLIN, "io_bazel_rules_kotlin")

def rules_kotlin_maven_artifacts_dict():
    artifacts_dict = {
        coordinates: KOTLIN_RELEASE_VERSION
        for coordinates in _JETBRAINS_KOTLIN_LIBRARIES
    }
    return artifacts_dict

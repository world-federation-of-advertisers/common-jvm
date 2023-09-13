# Copyright 2021 The Cross-Media Measurement Authors
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

"""
Repository rules/macros for Github GPRC Kotlin.
"""

load("//build:versions.bzl", "GRPC_KOTLIN", "versioned_http_archive")

# kt_jvm_grpc_library directly depends on these library targets.
GRPC_KOTLIN_OVERRIDE_TARGETS = {
    "io.grpc:grpc-kotlin-stub": "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:stub",
    "io.grpc:grpc-kotlin-context": "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:context",
}

def com_github_grpc_grpc_kotlin():
    versioned_http_archive(GRPC_KOTLIN, "com_github_grpc_grpc_kotlin")

def grpc_kotlin_maven_artifacts_dict():
    return {
        coordinates: GRPC_KOTLIN.version
        for coordinates in GRPC_KOTLIN_OVERRIDE_TARGETS.keys()
    }

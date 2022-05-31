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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("//build:versions.bzl", "GRPC_KOTLIN_VERSION")

# kt_jvm_grpc_library directly depends on the stub library target below.
GRPC_KOTLIN_OVERRIDE_TARGETS = {
    "io.grpc:grpc-kotlin-stub": "@com_github_grpc_grpc_kotlin//stub/src/main/java/io/grpc/kotlin:stub",
}

def com_github_grpc_grpc_kotlin():
    maybe(
        http_archive,
        name = "com_github_grpc_grpc_kotlin",
        sha256 = "466d33303aac7e825822b402efa3dcfddd68e6f566ed79443634180bb75eab6e",
        strip_prefix = "grpc-kotlin-{version}".format(version = GRPC_KOTLIN_VERSION),
        url = "https://github.com/grpc/grpc-kotlin/archive/refs/tags/v{version}.tar.gz".format(version = GRPC_KOTLIN_VERSION),
    )

def grpc_kotlin_maven_artifacts_dict():
    return {
        coordinates: GRPC_KOTLIN_VERSION
        for coordinates in GRPC_KOTLIN_OVERRIDE_TARGETS.keys()
    }

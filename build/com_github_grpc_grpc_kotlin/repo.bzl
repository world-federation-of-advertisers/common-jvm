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

def com_github_grpc_grpc_kotlin_repo():
    maybe(
        http_archive,
        name = "com_github_grpc_grpc_kotlin",
        sha256 = "fe0b50b833ce2c6edfdf6e98f45e02c162b936f89de55768173936103b3b11ce",
        strip_prefix = "grpc-kotlin-1.2.1",
        url = "https://github.com/grpc/grpc-kotlin/archive/refs/tags/v1.2.1.tar.gz",
    )

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

def com_github_grpc_grpc_kotlin_repo():
    if "com_github_grpc_grpc_kotlin" not in native.existing_rules():
        http_archive(
            name = "com_github_grpc_grpc_kotlin",
            sha256 = "fe0b50b833ce2c6edfdf6e98f45e02c162b936f89de55768173936103b3b11ce",
            strip_prefix = "grpc-kotlin-1.2.1",
            url = "https://github.com/grpc/grpc-kotlin/archive/refs/tags/v1.2.1.tar.gz",
        )
    if "io_grpc_grpc_java" not in native.existing_rules():
        http_archive(
            name = "io_grpc_grpc_java",
            sha256 = "51bac553d269b97214dbd6aee4e65fc616d8ccd414fc12d708e85979ed4c19b4",
            # Use latest grpc-java version
            strip_prefix = "grpc-java-1.45.1",
            url = "https://github.com/grpc/grpc-java/archive/refs/tags/v1.45.1.tar.gz",
        )

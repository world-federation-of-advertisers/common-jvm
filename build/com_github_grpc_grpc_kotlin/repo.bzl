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
            sha256 = "08f06a797ec806d68e8811018cefd1d5a6b8bf1782b63937f2618a6be86a9e2d",
            strip_prefix = "grpc-kotlin-0.2.1",
            url = "https://github.com/grpc/grpc-kotlin/archive/v0.2.1.zip",
        )

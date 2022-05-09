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
Repository rules/macros for Github GRPC.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def com_github_grpc_grpc_repo():
    if "com_github_grpc_grpc" not in native.existing_rules():
        http_archive(
            name = "com_github_grpc_grpc",
            sha256 = "67423a4cd706ce16a88d1549297023f0f9f0d695a96dd684adc21e67b021f9bc",
            strip_prefix = "grpc-1.46.0",
            urls = ["https://github.com/grpc/grpc/archive/v1.46.0.tar.gz"],
        )

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
Repository rules/macros for Protobuf.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//build:versions.bzl", "PROTOBUF_VERSION")

_URL_TEMPLATE = "https://github.com/protocolbuffers/protobuf/releases/download/v{version}/protobuf-all-{version}.tar.gz"

def com_google_protobuf_repo():
    http_archive(
        name = "com_google_protobuf",
        sha256 = "80631d5a18d51daa3a1336e340001ad4937e926762f21144c62d26fe2a8d71fe",
        strip_prefix = "protobuf-" + PROTOBUF_VERSION,
        url = _URL_TEMPLATE.format(version = PROTOBUF_VERSION),
    )

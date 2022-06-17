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

_URL_TEMPLATE = "https://github.com/protocolbuffers/protobuf/archive/v{version}.tar.gz"

def com_github_protocolbuffers_protobuf():
    http_archive(
        name = "com_github_protocolbuffers_protobuf",
        sha256 = "8b28fdd45bab62d15db232ec404248901842e5340299a57765e48abe8a80d930",
        strip_prefix = "protobuf-" + PROTOBUF_VERSION,
        url = _URL_TEMPLATE.format(version = PROTOBUF_VERSION),
    )

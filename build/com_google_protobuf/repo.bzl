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

COM_GOOGLE_PROTOBUF_VERSION = "3.17.3"

_URL_TEMPLATE = "https://github.com/protocolbuffers/protobuf/releases/download/v%s/protobuf-all-%s.tar.gz"

def com_google_protobuf_repo():
    http_archive(
        name = "com_google_protobuf",
        sha256 = "77ad26d3f65222fd96ccc18b055632b0bfedf295cb748b712a98ba1ac0b704b2",
        strip_prefix = "protobuf-" + COM_GOOGLE_PROTOBUF_VERSION,
        url = _URL_TEMPLATE % (COM_GOOGLE_PROTOBUF_VERSION, COM_GOOGLE_PROTOBUF_VERSION),
    )

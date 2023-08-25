# Copyright 2023 The Cross-Media Measurement Authors
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
Repository defs for Google's HighwayHash, necessary for Riegeli implementation
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def com_google_highwayhash():
    maybe(
        http_archive,
        name = "com_google_highwayhash",
        sha256 = "1e4e32f6198facbac7a35b04fa4c1acb5e6d9bb13f983c60903da9cbbbd9f5b5",
        url = "https://github.com/google/highwayhash/archive/a7f68e2f95fac08b24327d74747521cf634d5aff.tar.gz",
        build_file = Label("@wfa_common_jvm//build/com_google_highwayhash:BUILD.external"),
        strip_prefix = "highwayhash-a7f68e2f95fac08b24327d74747521cf634d5aff"
    )

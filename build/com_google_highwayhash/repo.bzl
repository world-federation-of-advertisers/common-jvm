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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def com_google_highwayhash():
    maybe(
        http_archive,
        name = "com_google_highwayhash",
        sha256 = "5380cb7cf19e7c9591f31792b7794d48084f6a3ab7c03d637cd6a32cf2ee8686",
        url = "https://github.com/google/highwayhash/archive/a7f68e2f95fac08b24327d74747521cf634d5aff.zip",
        build_file = Label("@wfa_common_jvm//build/com_google_highwayhash:BUILD.external")
    )

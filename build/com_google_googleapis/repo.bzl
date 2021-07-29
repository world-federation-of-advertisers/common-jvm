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
Repository rules/macros for Google APIs.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def com_google_googleapis_repo():
    if "com_google_googleapis" not in native.existing_rules():
        http_archive(
            name = "com_google_googleapis",
            sha256 = "65b3c3c4040ba3fc767c4b49714b839fe21dbe8467451892403ba90432bb5851",
            strip_prefix = "googleapis-a1af63efb82f54428ab35ea76869d9cd57ca52b8",
            urls = ["https://github.com/googleapis/googleapis/archive/a1af63efb82f54428ab35ea76869d9cd57ca52b8.tar.gz"],
        )

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
Repository rules/macros for JVM External Rules.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rules_jvm_external_repo():
    if "rules_jvm_external" not in native.existing_rules():
        http_archive(
            name = "rules_jvm_external",
            sha256 = "f36441aa876c4f6427bfb2d1f2d723b48e9d930b62662bf723ddfb8fc80f0140",
            strip_prefix = "rules_jvm_external-4.1",
            url = "https://github.com/bazelbuild/rules_jvm_external/archive/4.1.zip",
        )

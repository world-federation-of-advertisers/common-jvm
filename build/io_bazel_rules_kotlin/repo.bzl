# Copyright 2020 The Cross-Media Measurement Authors
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

"""Repository rules/macros for rules_kotlin.

See https://github.com/bazelbuild/rules_kotlin
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _rules_kotlin_repo(version, sha256):
    http_archive(
        name = "io_bazel_rules_kotlin",
        urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/%s/rules_kotlin_release.tgz" % version],
        sha256 = sha256,
    )

def rules_kotlin_repo():
    # Import rules_android to work around known bug in rules_kotlin v1.5.0-beta-2.
    # See https://github.com/bazelbuild/rules_kotlin/releases/tag/v1.5.0-beta-2
    if "rules_android" not in native.existing_rules():
        http_archive(
            name = "rules_android",
            urls = ["https://github.com/bazelbuild/rules_android/archive/v0.1.1.zip"],
            sha256 = "cd06d15dd8bb59926e4d65f9003bfc20f9da4b2519985c27e190cddc8b7a7806",
            strip_prefix = "rules_android-0.1.1",
        )

    _rules_kotlin_repo(
        version = "v1.5.0-beta-2",
        sha256 = "e4185409c787c18f332ae83a73827aab6e77058a48ffee0cac01123408cbc89a",
    )

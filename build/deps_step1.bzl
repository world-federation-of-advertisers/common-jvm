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
Adds external repos necessary for common-jvm.
"""

load("//build/bazel_skylib:repo.bzl", "bazel_skylib_repo")
load("//build/platforms:repo.bzl", "platforms_repo")
load("//build/com_google_protobuf:repo.bzl", "com_google_protobuf_repo")
load("//build/googletest:repo.bzl", "googletest_repo")
load("//build/com_google_absl:repo.bzl", "com_google_absl_repo")
load("//build/io_bazel_rules_kotlin:repo.bzl", "rules_kotlin_repo")

def common_jvm_deps_step1():
    """
    Adds all external repos necessary for common-cpp.
    """
    bazel_skylib_repo()
    platforms_repo()
    com_google_protobuf_repo()
    googletest_repo()
    com_google_absl_repo()

    rules_kotlin_repo()

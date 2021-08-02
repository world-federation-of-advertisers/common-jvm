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
load("//build/io_bazel_rules_kotlin:repo.bzl", "rules_kotlin_repo")
load("//build/com_github_grpc_grpc_kotlin:repo.bzl", "com_github_grpc_grpc_kotlin_repo")
load("//build/rules_jvm_external:repo.bzl", "rules_jvm_external_repo")
load("//build/com_github_grpc_grpc:repo.bzl", "com_github_grpc_grpc_repo")
load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")
load("//build/cue:repo.bzl", "cue_binaries")
load("//build/grpc_health_probe:repo.bzl", "grpc_health_probe_repo")
load("//build/io_bazel_rules_docker:repo.bzl", "rules_docker_repo")

def common_jvm_deps_repositories():
    """
    Adds all external repos necessary for common-jvm.
    """
    bazel_skylib_repo()
    platforms_repo()
    com_google_protobuf_repo()
    rules_kotlin_repo()
    com_github_grpc_grpc_kotlin_repo()
    rules_jvm_external_repo()
    com_github_grpc_grpc_repo()
    cloud_spanner_emulator_binaries(
        name = "cloud_spanner_emulator",
        sha256 = "7a3cdd5db7f5a427230ab67a8dc09cfcb6752dd7f0b28d51e8d08150b2641506",
        version = "1.1.1",
    )
    cue_binaries(
        name = "cue_binaries",
        sha256 = "810851e0e7d38192a6d0e09a6fa89ab5ff526ce29c9741f697995601edccb134",
        version = "0.2.2",
    )
    grpc_health_probe_repo()
    rules_docker_repo(
        name = "io_bazel_rules_docker",
        commit = "f929d80c5a4363994968248d87a892b1c2ef61d4",
        sha256 = "efda18e39a63ee3c1b187b1349f61c48c31322bf84227d319b5dece994380bb6",
    )

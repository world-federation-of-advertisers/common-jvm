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
load("//build/com_github_grpc_grpc_kotlin:repo.bzl", "com_github_grpc_grpc_kotlin_repo")
load("//build/rules_jvm_external:repo.bzl", "rules_jvm_external_repo")
load("//build/com_github_grpc_grpc:repo.bzl", "com_github_grpc_grpc_repo")
load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")
load("//build/cue:repo.bzl", "cue_binaries")
load("//build/grpc_health_probe:repo.bzl", "grpc_health_probe_repo")
load("//build/io_bazel_rules_docker:repo.bzl", "rules_docker_repo")
load("//build/com_google_googleapis:repo.bzl", "com_google_googleapis_repo")
load(
    "//build/io_bazel_rules_kotlin:repo.bzl",
    "rules_kotlin_repo",
)

def common_jvm_repositories():
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
    cloud_spanner_emulator_binaries()
    cue_binaries()
    grpc_health_probe_repo()
    rules_docker_repo()
    com_google_googleapis_repo()

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

load("//build/bazel_skylib:repo.bzl", "bazel_skylib")
load("//build/rules_pkg:repo.bzl", "rules_pkg")
load("//build/protobuf:repo.bzl", "com_github_protocolbuffers_protobuf")
load("//build/rules_proto:repo.bzl", "rules_proto")
load("//build/platforms:repo.bzl", "platforms_repo")
load("//build/grpc_java:repo.bzl", "io_grpc_grpc_java")
load("//build/grpc_kotlin:repo.bzl", "com_github_grpc_grpc_kotlin")
load("//build/rules_jvm_external:repo.bzl", "rules_jvm_external_repo")
load("//build/com_github_grpc_grpc:repo.bzl", "com_github_grpc_grpc")
load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")
load("//build/grpc_health_probe:repo.bzl", "grpc_health_probe")
load("//build/io_bazel_rules_docker:repo.bzl", "io_bazel_rules_docker")
load("//build/com_google_googleapis:repo.bzl", "com_google_googleapis")
load("//build/rules_kotlin:repo.bzl", "io_bazel_rules_kotlin")
load("//build/tink:repo.bzl", "tink_java")

def common_jvm_repositories():
    """
    Adds all external repos necessary for common-jvm.
    """
    platforms_repo()
    bazel_skylib()
    rules_pkg()
    com_github_protocolbuffers_protobuf()
    rules_proto()
    rules_jvm_external_repo()
    io_bazel_rules_kotlin()
    io_grpc_grpc_java()
    com_github_grpc_grpc_kotlin()
    com_github_grpc_grpc()
    cloud_spanner_emulator_binaries()
    grpc_health_probe()
    io_bazel_rules_docker()
    com_google_googleapis()
    tink_java()

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
load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")
load("//build/com_github_grpc_grpc:repo.bzl", "com_github_grpc_grpc")
load("//build/com_google_googleapis:repo.bzl", "com_google_googleapis")
load("//build/com_google_highwayhash:repo.bzl", "com_google_highwayhash")
load("//build/grpc_java:repo.bzl", "io_grpc_grpc_java")
load("//build/grpc_kotlin:repo.bzl", "com_github_grpc_grpc_kotlin")
load("//build/platforms:repo.bzl", "platforms_repo")
load("//build/protobuf:repo.bzl", "com_github_protocolbuffers_protobuf")
load("//build/rules_jvm_external:repo.bzl", "rules_jvm_external_repo")
load("//build/io_bazel_rules_go:repo.bzl", "io_bazel_rules_go")
load("//build/rules_kotlin:repo.bzl", "io_bazel_rules_kotlin")
load("//build/rules_multirun:repo.bzl", "rules_multirun")
load("//build/rules_oci:repo.bzl", "rules_oci")
load("//build/rules_pkg:repo.bzl", "rules_pkg")
load("//build/rules_proto:repo.bzl", "rules_proto")

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
    com_github_grpc_grpc()
    io_grpc_grpc_java()
    com_github_grpc_grpc_kotlin()
    cloud_spanner_emulator_binaries()
    rules_oci()
    rules_multirun()
    io_bazel_rules_go()
    com_google_googleapis()
    com_google_highwayhash()

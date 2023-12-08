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

load("@com_github_grpc_grpc_kotlin//:repositories.bzl", "grpc_kt_repositories")
load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")
load("@maven//:compat.bzl", "compat_repositories")
load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")
load("@rules_oci//oci:repositories.bzl", "LATEST_CRANE_VERSION", "oci_register_toolchains")
load("//build:grpc_extra_deps.bzl", "grpc_extra_deps")
load("//build/rules_oci:base_images.bzl", "base_java_images")

def common_jvm_extra_deps():
    """
    Adds second-level transitive dependencies of external repos for common-jvm.
    """
    rules_jvm_external_setup()
    grpc_extra_deps()
    compat_repositories()
    oci_register_toolchains(
        name = "oci",
        crane_version = LATEST_CRANE_VERSION,
    )
    base_java_images()
    grpc_kt_repositories()
    grpc_java_repositories()

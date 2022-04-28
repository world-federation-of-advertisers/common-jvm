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
load("//build/io_bazel_rules_kotlin:extra_deps.bzl", "rules_kotlin_extra_deps")
load("@com_github_grpc_grpc_kotlin//:repositories.bzl", "grpc_kt_repositories")
load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")
load("@maven//:compat.bzl", "compat_repositories")
load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")
load(
    "@io_bazel_rules_docker//java:image.bzl",
    java_image_repositories = "repositories",
)
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

def common_jvm_extra_deps():
    """
    Adds all external repos necessary for common-jvm.
    """
    rules_kotlin_extra_deps()
    compat_repositories()
    container_deps()
    java_image_repositories()
    grpc_extra_deps()
    grpc_kt_repositories()
    grpc_java_repositories()

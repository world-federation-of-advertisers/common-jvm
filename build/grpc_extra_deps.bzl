# Copyright 2023 The Cross-Media Measurement Authors
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
Loads the dependencies necessary for the external repositories defined in grpc_deps.bzl.
"""

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
load("@build_bazel_apple_support//lib:repositories.bzl", "apple_support_dependencies")
load("@build_bazel_rules_apple//apple:repositories.bzl", "apple_rules_dependencies")
load("@com_envoyproxy_protoc_gen_validate//:dependencies.bzl", "go_third_party")
load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
load("@envoy_api//bazel:repositories.bzl", "api_dependencies")
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")
load("@upb//bazel:workspace_deps.bzl", "upb_deps")

def grpc_extra_deps(ignore_version_differences = False, go_toolchains_version = "1.19.4"):
    """Loads additional gRPC dependencies.
    
    This is copied from https://github.com/grpc/grpc with modifications to address version compatibility issues. It must be run after `grpc_deps`.
    
    TODO(https://github.com/grpc/grpc/issues/32850): Revert when the dependency issue is addressed.

    Args:
      ignore_version_differences: Plumbed directly to the invocation of
        apple_rules_dependencies.
      go_toolchains_version: Plumbed directly to the invocation of 
        go_register_toolchains.
    """
    protobuf_deps()

    upb_deps()

    api_dependencies()

    go_rules_dependencies()
    go_register_toolchains(version = go_toolchains_version)
    gazelle_dependencies()

    # Pull-in the go 3rd party dependencies for protoc_gen_validate, which is
    # needed for building C++ xDS protos
    go_third_party()

    apple_rules_dependencies(ignore_version_differences = ignore_version_differences)

    apple_support_dependencies()

    # Initialize Google APIs with only C++ and Python targets
    switched_rules_by_language(
        name = "com_google_googleapis_imports",
        cc = True,
        grpc = True,
        python = True,
    )

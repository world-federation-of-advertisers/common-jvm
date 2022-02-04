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

"""Common macros."""

load(":defs.bzl", "to_label")
load("@rules_java//java:defs.bzl", "java_library", "java_proto_library")
load(
    "@com_github_grpc_grpc_kotlin//:kt_jvm_grpc.bzl",
    _kt_jvm_grpc_library = "kt_jvm_grpc_library",
)
load("//build/kt_jvm_proto:defs.bzl", "kt_jvm_proto_library")

def kt_jvm_grpc_library(
        name,
        srcs,
        deps,
        flavor = None,
        visibility = None,
        **kwargs):
    """Wrapper macro for regular kt_jvm_grpc_library rule from grpc_kotlin.

    This includes a convenience export for the java_proto_library dep.

    Args:
      name: Target name.
      srcs: Exactly one proto_library target.
      deps: Exactly one java_proto_library target.
      flavor: "normal" (default) for normal proto runtime, or "lite" for the
          lite runtime (for Android usage)
      visibility: List of visibility labels.
      **kwargs: Keyword arguments.
    """

    internal_name = name + "_internal"

    _kt_jvm_grpc_library(
        name = internal_name,
        srcs = srcs,
        deps = deps,
        flavor = flavor,
        visibility = ["//visibility:private"],
        **kwargs
    )
    java_library(
        name = name,
        exports = [
            internal_name,
        ] + deps,
        visibility = visibility,
        **kwargs
    )

def kt_jvm_grpc_proto_library(
        name,
        srcs,
        flavor = None,
        visibility = None,
        **kwargs):
    """java_library that exports Kotlin JVM gRPC service and proto libraries.

    Given a proto_library named `<prefix>_proto`, this will create additional
    `<prefix>_java_proto`, `<prefix>_kt_jvm_proto`, and `<prefix>_kt_jvm_grpc`
    targets.

    Args:
      name: Target name.
      srcs: Exactly one proto source file.
      flavor: "normal" (default) for normal proto runtime, or "lite" for the
          lite runtime (for Android usage)
      visibility: List of visibility labels.
      **kwargs: Keyword arguments.
    """

    if len(srcs) != 1:
        fail("Expected exactly one src", "srcs")

    proto_name = to_label(srcs[0]).name
    if not proto_name.endswith("_proto"):
        fail("proto_library target names should end with '_proto'")
    name_prefix = proto_name.rsplit("_", 1)[0]
    java_proto_name = name_prefix + "_java_proto"
    kt_jvm_proto_name = name_prefix + "_kt_jvm_proto"
    kt_jvm_grpc_name = name_prefix + "_kt_jvm_grpc"

    java_proto_library(
        name = java_proto_name,
        deps = srcs,
        visibility = visibility,
        **kwargs
    )

    kt_jvm_proto_library(
        name = kt_jvm_proto_name,
        srcs = srcs,
        deps = [java_proto_name],
        visibility = visibility,
        **kwargs
    )

    _kt_jvm_grpc_library(
        name = kt_jvm_grpc_name,
        srcs = srcs,
        deps = [java_proto_name],
        flavor = flavor,
        visibility = visibility,
        **kwargs
    )

    java_library(
        name = name,
        exports = [
            kt_jvm_grpc_name,
            kt_jvm_proto_name,
        ],
        visibility = visibility,
        **kwargs
    )

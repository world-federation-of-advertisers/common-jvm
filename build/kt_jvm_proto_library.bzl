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

load("@rules_proto//proto:defs.bzl", "ProtoInfo")

"""
Provides kt_jvm_proto_library to generate Kotlin protos.
"""

load("@io_bazel_rules_kotlin//kotlin:jvm.bzl", "kt_jvm_library")

def _get_real_short_path(file):
    # For some reason, files from other archives have short paths that look like:
    #   ../com_google_protobuf/google/protobuf/descriptor.proto
    short_path = file.short_path
    if short_path.startswith("../"):
        second_slash = short_path.index("/", 3)
        short_path = short_path[second_slash + 1:]

    # Sometimes it has another few prefixes like:
    #   _virtual_imports/any_proto/google/protobuf/any.proto
    #   benchmarks/_virtual_imports/100_msgs_proto/benchmarks/100_msgs.proto
    # We want just google/protobuf/any.proto.
    virtual_imports = "_virtual_imports/"
    if virtual_imports in short_path:
        short_path = short_path.split(virtual_imports)[1].split("/", 1)[1]
    return short_path

def _run_protoc(ctx, output_dir):
    """Executes protoc to generate Kotlin files."""
    proto_info = ctx.attr.proto_dep[ProtoInfo]
    descriptor_sets = proto_info.transitive_descriptor_sets
    transitive_descriptor_set = depset(transitive = [descriptor_sets])
    proto_sources = [_get_real_short_path(file) for file in proto_info.direct_sources]

    protoc_args = ctx.actions.args()
    protoc_args.set_param_file_format("multiline")
    protoc_args.use_param_file("@%s")
    protoc_args.add("--kotlin_out=" + output_dir.path)
    protoc_args.add_joined(
        transitive_descriptor_set,
        join_with = ctx.configuration.host_path_separator,
        format_joined = "--descriptor_set_in=%s",
    )
    protoc_args.add_all(proto_sources)
    ctx.actions.run(
        inputs = depset(transitive = [transitive_descriptor_set]),
        outputs = [output_dir],
        executable = ctx.executable._protoc,
        arguments = [protoc_args],
        mnemonic = "KtProtoc",
        progress_message = "Generating Kotlin protos for " + ctx.label.name,
    )

def _create_src_jar(ctx, java_runtime_info, input_dir, output_jar):
    """Bundles .kt files into a srcjar."""
    jar_args = ctx.actions.args()
    jar_args.add("cf", output_jar)
    jar_args.add_all([input_dir])

    ctx.actions.run(
        outputs = [output_jar],
        inputs = [input_dir],
        executable = "%s/bin/jar" % java_runtime_info.java_home,
        tools = java_runtime_info.files,
        arguments = [jar_args],
        mnemonic = "KtProtoSrcJar",
        progress_message = "Generating Kotlin proto srcjar for " + ctx.label.name,
    )

def _kt_jvm_proto_library_helper_impl(ctx):
    """Implementation of _kt_jvm_proto_library_helper rule."""
    name = ctx.label.name
    java_runtime = ctx.attr._jdk[java_common.JavaRuntimeInfo]

    gen_src_dir_name = "%s/ktproto" % name
    gen_src_dir = ctx.actions.declare_directory(gen_src_dir_name)

    _run_protoc(ctx, gen_src_dir)
    _create_src_jar(ctx, java_runtime, gen_src_dir, ctx.outputs.srcjar)

_kt_jvm_proto_library_helper = rule(
    attrs = {
        "java_proto_dep": attr.label(providers = [JavaInfo]),
        "proto_dep": attr.label(providers = [ProtoInfo]),
        "srcjar": attr.output(
            doc = "Generated Java source jar.",
            mandatory = True,
        ),
        "_jdk": attr.label(
            default = Label("@bazel_tools//tools/jdk:current_java_runtime"),
            providers = [java_common.JavaRuntimeInfo],
        ),
        "_protoc": attr.label(
            default = Label("@com_google_protobuf//:protoc"),
            cfg = "host",
            executable = True,
        ),
    },
    implementation = _kt_jvm_proto_library_helper_impl,
)

def kt_jvm_proto_library(name, srcs = None, deps = None, **kwargs):
    """Generates Kotlin code for a protocol buffer library.

    For standard attributes, see:
      https://docs.bazel.build/versions/master/be/common-definitions.html#common-attributes

    Args:
      name: A name for the target
      srcs: Exactly one proto_library target to generate Kotlin APIs for
      deps: Exactly one java_proto_library target for srcs[0]
      **kwargs: other args to pass to the ultimate kt_jvm_library target
    """
    srcs = srcs or []
    deps = deps or []

    if len(srcs) != 1:
        fail("Expected exactly one src", "srcs")

    if len(deps) != 1:
        fail("Expected exactly one dep", "deps")

    generated_kt_name = name + "_DO_NOT_DEPEND_generated_kt"
    generated_srcjar = generated_kt_name + ".srcjar"
    _kt_jvm_proto_library_helper(
        name = generated_kt_name,
        java_proto_dep = deps[0],
        proto_dep = srcs[0],
        srcjar = generated_srcjar,
        visibility = ["//visibility:private"],
    )

    kt_jvm_library(
        name = name,
        srcs = [generated_srcjar],
        # TODO: add Bazel rule in protobuf instead of relying on Maven
        deps = deps + ["@maven//:com_google_protobuf_protobuf_kotlin"],
        **kwargs
    )

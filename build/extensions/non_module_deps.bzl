# Copyright 2023 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module extension for non-module dependencies of common_jvm."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")
load("//build/com_google_googleapis:repo.bzl", "com_google_googleapis")
load("//build/com_google_highwayhash:repo.bzl", "com_google_highwayhash")

_file_version_tag = tag_class(
    attrs = {
        "version": attr.string(),
        "sha256": attr.string(),
    },
)

def _non_module_deps_impl(mctx):
    tink_java_version = None
    for module in mctx.modules:
        for file_version in module.tags.tink_java_version:
            if tink_java_version:
                fail("Only one tink-java version supported")
            tink_java_version = file_version

    # TODO(tink-crypto/tink-java#19): Remove when fixed.
    if not tink_java_version:
        fail("tink_java_version is required")
    http_archive(
        name = "tink_java_src",
        urls = ["https://github.com/tink-crypto/tink-java/archive/refs/tags/v{version}.zip".format(version = tink_java_version.version)],
        strip_prefix = "tink-java-{version}/src/main/java/com/google/crypto/tink".format(version = tink_java_version.version),
        sha256 = tink_java_version.sha256,
        build_file_content = """
load("@rules_java//java:defs.bzl", "java_library")

package(default_visibility = ["//visibility:public"])
            
java_library(
    name = "kms_clients_test_util",
    srcs = ["KmsClientsTestUtil.java"],
    deps = ["@wfa_common_jvm//imports/java/com/google/crypto/tink"],
    testonly = True,
)
""",
    )

    com_google_googleapis()
    cloud_spanner_emulator_binaries()
    com_google_highwayhash()

    http_archive(
        name = "io_grpc_grpc_proto",
        sha256 = "464e97a24d7d784d9c94c25fa537ba24127af5aae3edd381007b5b98705a0518",
        strip_prefix = "grpc-proto-08911e9d585cbda3a55eb1dcc4b99c89aebccff8",
        urls = ["https://github.com/grpc/grpc-proto/archive/08911e9d585cbda3a55eb1dcc4b99c89aebccff8.zip"],
    )

non_module_deps = module_extension(
    implementation = _non_module_deps_impl,
    tag_classes = {
        "tink_java_version": _file_version_tag,
    },
)

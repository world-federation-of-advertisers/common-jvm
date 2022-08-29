# Copyright 2022 The Cross-Media Measurement Authors
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

"""Repository rules/macros for grpc-java."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("//build:versions.bzl", "GRPC_JAVA_VERSION")

# Known coordinates for non-Android Maven artifacts built from grpc-java.
_MAVEN_COORDINATES = [
    "io.grpc:grpc-alts",
    "io.grpc:grpc-api",
    "io.grpc:grpc-auth",
    "io.grpc:grpc-benchmarks",
    "io.grpc:grpc-bom",
    "io.grpc:grpc-census",
    "io.grpc:grpc-context",
    "io.grpc:grpc-core",
    "io.grpc:grpc-googleapis",
    "io.grpc:grpc-grpclb",
    "io.grpc:grpc-netty",
    "io.grpc:grpc-netty-shaded",
    "io.grpc:grpc-okhttp",
    "io.grpc:grpc-protobuf",
    "io.grpc:grpc-protobuf-lite",
    "io.grpc:grpc-rls",
    "io.grpc:grpc-services",
    "io.grpc:grpc-stub",
    "io.grpc:grpc-testing",
    "io.grpc:grpc-xds",
]

def io_grpc_grpc_java():
    maybe(
        http_archive,
        name = "io_grpc_grpc_java",
        sha256 = "88b12b2b4e0beb849eddde98d5373f2f932513229dbf9ec86cc8e4912fc75e79",
        strip_prefix = "grpc-java-{version}".format(version = GRPC_JAVA_VERSION),
        url = "https://github.com/grpc/grpc-java/archive/refs/tags/v{version}.tar.gz".format(version = GRPC_JAVA_VERSION),
    )

def grpc_java_maven_artifacts_dict():
    return {coordinates: GRPC_JAVA_VERSION for coordinates in _MAVEN_COORDINATES}

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

load("//build:versions.bzl", "GRPC_JAVA", "versioned_http_archive")

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
    versioned_http_archive(GRPC_JAVA, "io_grpc_grpc_java")

def grpc_java_maven_artifacts_dict():
    return {
        coordinates: GRPC_JAVA.version
        for coordinates in _MAVEN_COORDINATES
    }

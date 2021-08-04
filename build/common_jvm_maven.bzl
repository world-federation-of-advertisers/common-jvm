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

load(
    "//build/io_bazel_rules_kotlin:repo.bzl",
    "IO_BAZEL_RULES_KOTLIN_OVERRIDE_TARGETS",
)
load(
    "@com_github_grpc_grpc_kotlin//:repositories.bzl",
    "IO_GRPC_GRPC_KOTLIN_ARTIFACTS",
    "IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS",
)
load(
    "@io_grpc_grpc_java//:repositories.bzl",
    "IO_GRPC_GRPC_JAVA_ARTIFACTS",
    "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS",
)
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")
load("//build/kotlinx_coroutines:repo.bzl", "kotlinx_coroutines_artifact_dict")
load("//build/maven:artifacts.bzl", "artifacts")

def common_jvm_maven_artifacts():
    """
    Adds external repos necessary for common-jvm.

    Returns:
        An updated dictionary from a list of Java and Kotlin artifacts
    """
    maven_artifacts = artifacts.list_to_dict(
        IO_GRPC_GRPC_JAVA_ARTIFACTS +
        IO_GRPC_GRPC_KOTLIN_ARTIFACTS,
    )
    maven_artifacts.update(com_google_truth_artifact_dict(version = "1.0.1"))

    # kotlinx.coroutines version should be compatible with Kotlin release used by
    # rules_kotlin. See https://kotlinlang.org/docs/releases.html#release-details.
    maven_artifacts.update(kotlinx_coroutines_artifact_dict(version = "1.4.3"))

    # Add Maven artifacts or override versions (e.g. those pulled in by gRPC Kotlin
    # or default dependency versions).
    maven_artifacts.update({
        "com.google.api.grpc:grpc-google-cloud-pubsub-v1": "0.1.24",
        "com.google.cloud:google-cloud-nio": "0.122.0",
        "com.google.cloud:google-cloud-spanner": "3.0.3",
        "com.google.code.gson:gson": "2.8.6",
        "com.google.guava:guava": "30.0-jre",
        "info.picocli:picocli": "4.4.0",
        "junit:junit": "4.13",
        "org.conscrypt:conscrypt-openjdk-uber": "2.5.2",
        "org.mockito.kotlin:mockito-kotlin": "3.2.0",

        # For grpc-kotlin. This should be a version that is compatible with the
        # Kotlin release used by rules_kotlin.
        "com.squareup:kotlinpoet": "1.8.0",
    })

    return artifacts.dict_to_list(maven_artifacts)

COMMON_JVM_MAVEN_TARGETS = dict(
    IO_BAZEL_RULES_KOTLIN_OVERRIDE_TARGETS.items() +
    IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS.items() +
    IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS.items(),
)

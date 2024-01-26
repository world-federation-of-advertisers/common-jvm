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
    "@com_github_grpc_grpc_kotlin//:repositories.bzl",
    GRPC_KOTLIN_MAVEN_DEPS = "IO_GRPC_GRPC_KOTLIN_ARTIFACTS",
)
load(
    "@io_grpc_grpc_java//:repositories.bzl",
    GRPC_JAVA_MAVEN_DEPS = "IO_GRPC_GRPC_JAVA_ARTIFACTS",
)
load(
    "//build:versions.bzl",
    "AWS_JAVA_SDK_VERSION",
    "OPENTELEMETRY_JAVA_VERSION",
    "PROTOBUF_KOTLIN_VERSION",
)
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")
load("//build/grpc_java:repo.bzl", "grpc_java_maven_artifacts_dict")
load(
    "//build/grpc_kotlin:repo.bzl",
    "GRPC_KOTLIN_OVERRIDE_TARGETS",
    "grpc_kotlin_maven_artifacts_dict",
)
load("//build/kotlinx_coroutines:repo.bzl", "kotlinx_coroutines_artifact_dict")
load("//build/maven:artifacts.bzl", "artifacts")
load(
    "//build/rules_kotlin:repo.bzl",
    "rules_kotlin_maven_artifacts_dict",
)
load(
    "//build/rules_proto:repo.bzl",
    "rules_proto_maven_artifacts_dict",
    RULES_PROTO_EXCLUDED_ARTIFACTS = "EXCLUDED_ARTIFACTS",
)

# buildifier: disable=function-docstring-return
def common_jvm_maven_artifacts():
    """Returns a list of Maven artifacts for this repo."""
    return artifacts.dict_to_list(common_jvm_maven_artifacts_dict())

# buildifier: disable=function-docstring-return
def common_jvm_maven_artifacts_dict():
    """Returns a dict of Maven coordinates to version for this repo."""
    maven_artifacts = artifacts.list_to_dict(
        GRPC_JAVA_MAVEN_DEPS +
        GRPC_KOTLIN_MAVEN_DEPS,
    )

    maven_artifacts.update(rules_proto_maven_artifacts_dict())
    maven_artifacts.update(rules_kotlin_maven_artifacts_dict())
    maven_artifacts.update(grpc_java_maven_artifacts_dict())
    maven_artifacts.update(grpc_kotlin_maven_artifacts_dict())
    maven_artifacts.update(com_google_truth_artifact_dict(version = "1.0.1"))
    maven_artifacts.update(kotlinx_coroutines_artifact_dict())

    # Add Maven artifacts or override versions (e.g. those pulled in by gRPC Kotlin
    # or default dependency versions).
    maven_artifacts.update({
        "com.adobe.testing:s3mock-junit4": "2.2.3",
        "com.google.cloud:google-cloud-bigquery": "2.10.10",
        "com.google.cloud:google-cloud-nio": "0.123.28",
        "com.google.cloud:google-cloud-spanner": "6.35.2",
        "com.google.cloud:google-cloud-storage": "2.6.1",
        "com.google.guava:guava": "31.0.1-jre",
        "com.google.crypto.tink:tink": "1.12.0",
        "info.picocli:picocli": "4.4.0",
        "io.opentelemetry:opentelemetry-api": OPENTELEMETRY_JAVA_VERSION,
        "junit:junit": "4.13",
        "org.conscrypt:conscrypt-openjdk-uber": "2.5.2",
        "org.jetbrains:annotations": "23.0.0",
        "org.mockito.kotlin:mockito-kotlin": "3.2.0",
        "software.amazon.awssdk:s3": AWS_JAVA_SDK_VERSION,
        "software.amazon.awssdk:secretsmanager": AWS_JAVA_SDK_VERSION,
        "software.amazon.awssdk:sts": AWS_JAVA_SDK_VERSION,
        "software.amazon.awssdk:regions": AWS_JAVA_SDK_VERSION,

        # PostgreSQL.
        "com.google.cloud.sql:postgres-socket-factory": "1.13.0",
        "com.google.cloud.sql:cloud-sql-connector-r2dbc-postgres": "1.13.0",
        "org.postgresql:postgresql": "42.7.1",
        "org.postgresql:r2dbc-postgresql": "1.0.4.RELEASE",
        "org.testcontainers:postgresql": "1.19.4",

        # Liquibase.
        "org.yaml:snakeyaml": "1.30",
        "org.liquibase:liquibase-core": "4.18.0",
        "com.google.cloudspannerecosystem:liquibase-spanner": "4.17.0",
        "com.google.cloud:google-cloud-spanner-jdbc": "2.9.0",
        "org.liquibase.ext:liquibase-postgresql": "4.11.0",

        # For grpc-kotlin. This should be a version that is compatible with
        # KOTLIN_RELEASE_VERSION.
        "com.squareup:kotlinpoet": "1.8.0",

        # For kt_jvm_proto_library.
        "com.google.protobuf:protobuf-kotlin": PROTOBUF_KOTLIN_VERSION,

        # Math library.
        "org.apache.commons:commons-math3": "3.6.1",
        "org.apache.commons:commons-numbers-gamma": "1.1",

        # CSV library.
        "com.opencsv:opencsv": "5.6",

        # Riegeli Decompressor
        "org.apache.commons:commons-compress": "1.22",
        "org.brotli:dec": "0.1.2",
        "com.github.luben:zstd-jni": "1.5.2-5",
    })

    return maven_artifacts

COMMON_JVM_MAVEN_OVERRIDE_TARGETS = GRPC_KOTLIN_OVERRIDE_TARGETS

COMMON_JVM_EXCLUDED_ARTIFACTS = [
    # Until the log2shell has been more widely mitigated, prohibit log4j totally.
    "org.apache.logging.log4j:log4j",
    "org.apache.logging.log4j:log4j-api",
    "org.apache.logging.log4j:log4j-core",
    "org.slf4j:log4j-over-slf4j",
    "org.slf4j:slf4j-log4j12",
] + RULES_PROTO_EXCLUDED_ARTIFACTS

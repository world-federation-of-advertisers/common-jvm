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
    "//build/rules_kotlin:repo.bzl",
    "RULES_KOTLIN_OVERRIDE_TARGETS",
    "rules_kotlin_maven_artifacts_dict",
)
load(
    "@io_grpc_grpc_java//:repositories.bzl",
    GRPC_JAVA_MAVEN_DEPS = "IO_GRPC_GRPC_JAVA_ARTIFACTS",
)
load(
    "@com_github_grpc_grpc_kotlin//:repositories.bzl",
    GRPC_KOTLIN_MAVEN_DEPS = "IO_GRPC_GRPC_KOTLIN_ARTIFACTS",
)
load("//build/rules_proto:repo.bzl", "rules_proto_maven_artifacts_dict")
load("//build/grpc_java:repo.bzl", "grpc_java_maven_artifacts_dict")
load(
    "//build/grpc_kotlin:repo.bzl",
    "GRPC_KOTLIN_OVERRIDE_TARGETS",
    "grpc_kotlin_maven_artifacts_dict",
)
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")
load("//build/kotlinx_coroutines:repo.bzl", "kotlinx_coroutines_artifact_dict")
load("//build/maven:artifacts.bzl", "artifacts")
load("//build/tink:repo.bzl", "TINK_JAVA_KMS_MAVEN_DEPS")
load("//build:versions.bzl", "PROTOBUF_KOTLIN_VERSION")

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
    maven_artifacts.update(TINK_JAVA_KMS_MAVEN_DEPS)
    maven_artifacts.update(com_google_truth_artifact_dict(version = "1.0.1"))
    maven_artifacts.update(kotlinx_coroutines_artifact_dict())

    # Add Maven artifacts or override versions (e.g. those pulled in by gRPC Kotlin
    # or default dependency versions).
    maven_artifacts.update({
        "com.adobe.testing:s3mock-junit4": "2.2.3",
        "com.google.cloud:google-cloud-bigquery": "2.10.10",
        "com.google.cloud:google-cloud-nio": "0.123.28",
        "com.google.cloud:google-cloud-spanner": "6.27.0",
        "com.google.cloud:google-cloud-storage": "2.6.1",
        "com.google.guava:guava": "31.0.1-jre",
        "info.picocli:picocli": "4.4.0",
        "junit:junit": "4.13",
        "org.conscrypt:conscrypt-openjdk-uber": "2.5.2",
        "org.jetbrains:annotations": "23.0.0",
        "org.mockito.kotlin:mockito-kotlin": "3.2.0",
        "software.amazon.awssdk:s3": "2.17.258",

        # PostgreSQL.
        "com.google.cloud.sql:postgres-socket-factory": "1.6.2",
        "com.google.cloud.sql:cloud-sql-connector-r2dbc-postgres": "1.6.2",
        "org.postgresql:postgresql": "42.4.0",
        "org.postgresql:r2dbc-postgresql": "0.9.1.RELEASE",
        "com.opentable.components:otj-pg-embedded": "1.0.1",

        # Liquibase.
        "org.liquibase:liquibase-core": "4.15.0",
        "org.yaml:snakeyaml": "1.30",
        "com.google.cloudspannerecosystem:liquibase-spanner": "4.10.0",
        "com.google.cloud:google-cloud-spanner-jdbc": "2.7.5",
        "org.liquibase.ext:liquibase-postgresql": "4.11.0",

        # For grpc-kotlin. This should be a version that is compatible with
        # KOTLIN_RELEASE_VERSION.
        "com.squareup:kotlinpoet": "1.8.0",

        # For kt_jvm_proto_library.
        "com.google.protobuf:protobuf-kotlin": PROTOBUF_KOTLIN_VERSION,

        # Math library.
        "org.apache.commons:commons-math3": "3.6.1",

        # CSV library.
        "com.opencsv:opencsv": "5.6",

        # Riegeli Decompressor
        "org.apache.commons:commons-compress": "1.22",
        "org.brotli:dec": "0.1.2",
    })

    return maven_artifacts

COMMON_JVM_MAVEN_OVERRIDE_TARGETS = dict(RULES_KOTLIN_OVERRIDE_TARGETS.items() + GRPC_KOTLIN_OVERRIDE_TARGETS.items())

# Until the log2shell has been more widely mitigated, prohibit log4j totally.
COMMON_JVM_EXCLUDED_ARTIFACTS = [
    "org.apache.logging.log4j:log4j",
    "org.apache.logging.log4j:log4j-api",
    "org.apache.logging.log4j:log4j-core",
    "org.slf4j:log4j-over-slf4j",
    "org.slf4j:slf4j-log4j12",
]

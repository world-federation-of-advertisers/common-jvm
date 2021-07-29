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
    "grpc_kt_repositories",
)
load(
    "@io_grpc_grpc_java//:repositories.bzl",
    "IO_GRPC_GRPC_JAVA_ARTIFACTS",
    "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS",
    "grpc_java_repositories",
)
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")
load("//build/kotlinx_coroutines:repo.bzl", "kotlinx_coroutines_artifact_dict")
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("//build/maven:artifacts.bzl", "artifacts")

def common_jvm_maven():
    MAVEN_ARTIFACTS = artifacts.list_to_dict(
        IO_GRPC_GRPC_JAVA_ARTIFACTS +
        IO_GRPC_GRPC_KOTLIN_ARTIFACTS,
    )
    MAVEN_ARTIFACTS.update(com_google_truth_artifact_dict(version = "1.0.1"))

    # kotlinx.coroutines version should be compatible with Kotlin release used by
    # rules_kotlin. See https://kotlinlang.org/docs/releases.html#release-details.
    MAVEN_ARTIFACTS.update(kotlinx_coroutines_artifact_dict(version = "1.4.3"))

    # Add Maven artifacts or override versions (e.g. those pulled in by gRPC Kotlin
    # or default dependency versions).
    MAVEN_ARTIFACTS.update({
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

    maven_install(
        artifacts = artifacts.dict_to_list(MAVEN_ARTIFACTS),
        fetch_sources = True,
        generate_compat_repositories = True,
        override_targets = dict(
            IO_BAZEL_RULES_KOTLIN_OVERRIDE_TARGETS.items() +
            IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS.items() +
            IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS.items(),
        ),
        repositories = [
            "https://repo.maven.apache.org/maven2/",
        ],
    )

    grpc_kt_repositories()
    grpc_java_repositories()

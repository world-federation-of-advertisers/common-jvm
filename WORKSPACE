workspace(name = "wfa_common_jvm")

load("//build:deps_step1.bzl", "common_jvm_deps_step1")

common_jvm_deps_step1()

load(
    "//build/io_bazel_rules_kotlin:repo.bzl",
    "IO_BAZEL_RULES_KOTLIN_OVERRIDE_TARGETS",
    "rules_kotlin_repo",
)

rules_kotlin_repo()

load("//build:deps_step2.bzl", "common_jvm_deps_step2")

common_jvm_deps_step2()

load(
    "@com_github_grpc_grpc_kotlin//:repositories.bzl",
    "IO_GRPC_GRPC_KOTLIN_ARTIFACTS",
    "IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS",
    "grpc_kt_repositories",
    "io_grpc_grpc_java",
)

io_grpc_grpc_java()

load(
    "@io_grpc_grpc_java//:repositories.bzl",
    "IO_GRPC_GRPC_JAVA_ARTIFACTS",
    "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS",
    "grpc_java_repositories",
)

# @com_google_truth_truth
load("//build/com_google_truth:repo.bzl", "com_google_truth_artifact_dict")

# kotlinx.coroutines
load("//build/kotlinx_coroutines:repo.bzl", "kotlinx_coroutines_artifact_dict")

# Maven

load("//build/rules_jvm_external:repo.bzl", "rules_jvm_external_repo")

rules_jvm_external_repo()

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("//build/maven:artifacts.bzl", "artifacts")

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

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()

# Run after compat_repositories to ensure the maven_install-selected
# dependencies are used.
grpc_kt_repositories()

grpc_java_repositories()  # For gRPC Kotlin.

# @io_bazel_rules_docker

load("//build/io_bazel_rules_docker:repo.bzl", "rules_docker_repo")

rules_docker_repo(
    name = "io_bazel_rules_docker",
    commit = "f929d80c5a4363994968248d87a892b1c2ef61d4",
    sha256 = "efda18e39a63ee3c1b187b1349f61c48c31322bf84227d319b5dece994380bb6",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("//build/io_bazel_rules_docker:base_images.bzl", "base_java_images")

# Defualt base images for java_image targets. Must come before
# java_image_repositories().
base_java_images(
    # gcr.io/distroless/java:11-debug
    debug_digest = "sha256:c3fe781de55d375de2675c3f23beb3e76f007e53fed9366ba931cc6d1df4b457",
    # gcr.io/distroless/java:11
    digest = "sha256:7fc091e8686df11f7bf0b7f67fd7da9862b2b9a3e49978d1184f0ff62cb673cc",
)

load(
    "@io_bazel_rules_docker//java:image.bzl",
    java_image_repositories = "repositories",
)

java_image_repositories()

# gRPC

load("//build/com_github_grpc_grpc:repo.bzl", "com_github_grpc_grpc_repo")

com_github_grpc_grpc_repo()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

# @cloud_spanner_emulator

load("//build/cloud_spanner_emulator:defs.bzl", "cloud_spanner_emulator_binaries")

cloud_spanner_emulator_binaries(
    name = "cloud_spanner_emulator",
    sha256 = "7a3cdd5db7f5a427230ab67a8dc09cfcb6752dd7f0b28d51e8d08150b2641506",
    version = "1.1.1",
)

# CUE binaries.

load("//build/cue:repo.bzl", "cue_binaries")

cue_binaries(
    name = "cue_binaries",
    sha256 = "810851e0e7d38192a6d0e09a6fa89ab5ff526ce29c9741f697995601edccb134",
    version = "0.2.2",
)

load("//build:deps_step3.bzl", "common_jvm_deps_step3")

common_jvm_deps_step3()

# Google APIs imports. Required to build googleapis.
load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    java = True,
)

workspace(name = "wfa_common_jvm")

load("//build:common_jvm_repositories.bzl", "common_jvm_repositories")

common_jvm_repositories()

load("//build:common_jvm_deps.bzl", "common_jvm_deps")

common_jvm_deps()

load(
    "//build:common_jvm_maven.bzl",
    "COMMON_JVM_EXCLUDED_ARTIFACTS",
    "COMMON_JVM_MAVEN_OVERRIDE_TARGETS",
    "common_jvm_maven_artifacts",
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = common_jvm_maven_artifacts(),
    excluded_artifacts = COMMON_JVM_EXCLUDED_ARTIFACTS,
    fetch_sources = True,
    generate_compat_repositories = True,
    override_targets = COMMON_JVM_MAVEN_OVERRIDE_TARGETS,
    repositories = ["https://repo.maven.apache.org/maven2/"],
)

load("//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()

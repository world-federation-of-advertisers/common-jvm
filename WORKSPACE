workspace(name = "wfa_common_jvm")

load("//build:common_jvm_repositories.bzl", "common_jvm_deps_repositories")

common_jvm_deps_repositories()

load("//build:common_jvm_deps.bzl", "common_jvm_deps")

common_jvm_deps()

load("//build:common_jvm_maven.bzl", "common_jvm_maven_artifacts", "common_jvm_maven_targets")
load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = common_jvm_maven_artifacts(),
    fetch_sources = True,
    generate_compat_repositories = True,
    override_targets = common_jvm_maven_targets,
    repositories = [
        "https://repo.maven.apache.org/maven2/",
    ],
)

load("//build:common_jvm_extra_deps.bzl", "common_jvm_extra_deps")

common_jvm_extra_deps()

"""
Adds external repos necessary for common-jvm.
"""

load("@maven//:compat.bzl", "compat_repositories")
load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")
load(
    "@io_bazel_rules_docker//java:image.bzl",
    java_image_repositories = "repositories",
)
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

def common_jvm_deps_step3():
    compat_repositories()
    container_deps()
    java_image_repositories()
    grpc_extra_deps()

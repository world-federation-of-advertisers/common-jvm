"""
Adds external repos necessary for common-jvm.
"""

load("//build/io_bazel_rules_kotlin:deps.bzl", "rules_kotlin_deps")
load("//build/com_github_grpc_grpc_kotlin:repo.bzl", "com_github_grpc_grpc_kotlin_repo")

def common_jvm_deps_step2():
    rules_kotlin_deps()
    com_github_grpc_grpc_kotlin_repo()

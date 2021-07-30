"""
Adds external repos necessary for common-jvm.
"""

load("//build/grpc_health_probe:repo.bzl", "grpc_health_probe_repo")
load("//build/com_google_googleapis:repo.bzl", "com_google_googleapis_repo")

def common_jvm_deps_step3():
    grpc_health_probe_repo()
    com_google_googleapis_repo()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def com_github_grpc_grpc_repo():
    if "com_github_grpc_grpc" not in native.existing_rules():
        http_archive(
            name = "com_github_grpc_grpc",
            sha256 = "8eb9d86649c4d4a7df790226df28f081b97a62bf12c5c5fe9b5d31a29cd6541a",
            strip_prefix = "grpc-1.36.4",
            urls = ["https://github.com/grpc/grpc/archive/v1.36.4.tar.gz"],
        )

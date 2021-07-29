load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

def grpc_health_probe_repo():
    if "grpc_health_probe" not in native.existing_rules():
        http_file(
            name = "grpc_health_probe",
            downloaded_file_path = "grpc-health-probe",
            executable = True,
            sha256 = "c78e988a4aad5e9e599c6a69e681ac68579c000b8f0571593325ccbc0c1638b7",
            urls = [
                "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.6/grpc_health_probe-linux-amd64",
            ],
        )

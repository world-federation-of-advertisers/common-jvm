# Copyright 2020 The Cross-Media Measurement Authors
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

"""Build defs for base container images."""

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

def base_java_images():
    """Default base Java image targets.

    These must come before calling repositories() in
    @io_bazel_rules_docker//java:image.bzl. The target names are significant.

    See https://console.cloud.google.com/gcr/images/distroless/GLOBAL/java

    We currently use
    gcr.io/distroless/java11-debian11:nonroot
    and
    gcr.io/distroless/java11-debian11:debug-nonroot
    as the base images.
    """

    container_pull(
        name = "java_image_base",
        digest = "sha256:a9be9ef912e263a1cf386a91648ee2454b892e5607ddf285875a5c4e2b0079b3",
        registry = "gcr.io",
        repository = "distroless/java11-debian11",
    )

    container_pull(
        name = "java_debug_image_base",
        digest = "sha256:b33c5d712678985705cd85d884daf4444333a00f590fc7c48a7b8165c5a902a8",
        registry = "gcr.io",
        repository = "distroless/java11-debian11",
    )

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
    gcr.io/distroless/java11-debian11:nonroot-amd64
    and
    gcr.io/distroless/java11-debian11:debug-nonroot-amd64
    as the base images.
    """

    container_pull(
        name = "java_image_base",
        digest = "sha256:6652063a30969dd5380cb5b34f36a7f9d2a682af6ad81d0a7ca8113d6e8d3d5c",
        registry = "gcr.io",
        repository = "distroless/java11-debian11",
    )

    container_pull(
        name = "java_debug_image_base",
        digest = "sha256:f9231c515b187ea1b586858003ad2292d6289447415caccb1d91e85adfb5a47a",
        registry = "gcr.io",
        repository = "distroless/java11-debian11",
    )

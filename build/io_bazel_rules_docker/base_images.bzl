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

def base_java_images(name):
    """Default base Java image targets.

    These must come before calling repositories() in
    @io_bazel_rules_docker//java:image.bzl. The target names are significant.

    See https://console.cloud.google.com/gcr/images/distroless/GLOBAL/java

    We currently use
    gcr.io/distroless/java:11-nonroot
    and
    gcr.io/distroless/java:11-debug-nonroot
    as the base images.
    """

    container_pull(
        name = "java_image_base",
        digest = "sha256:350d756ddcaf819b582ba6a58c4425a1db78e5798e53355f04a235cd7e0da4eb",
        registry = "gcr.io",
        repository = "distroless/java",
    )

    container_pull(
        name = "java_debug_image_base",
        digest = "sha256:817930976e739c52dffc9fc7ee37dbdfc1125639db90ab9c9aab62f793a9aa9a",
        registry = "gcr.io",
        repository = "distroless/java",
    )

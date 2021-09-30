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
    gcr.io/distroless/java-debian11:11-nonroot
    and
    gcr.io/distroless/java-debian11:11-debug-nonroot
    as the base images.
    """

    container_pull(
        name = "java_image_base",
        digest = "sha256:12bfe4f3300b164b3b4adb0ed8c78e81fa5f49850c1882cca89fe70dc5deb8d4",
        registry = "gcr.io",
        repository = "distroless/java",
    )

    container_pull(
        name = "java_debug_image_base",
        digest = "sha256:bae3c77dc7f1500a252d17c101dbd4ae9324114a05d51c12451a42ff7961b54f",
        registry = "gcr.io",
        repository = "distroless/java",
    )

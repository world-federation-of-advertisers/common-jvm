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

load("@rules_oci//oci:pull.bzl", "oci_pull")

def base_java_images():
    """Default base Java image targets."""

    oci_pull(
        name = "java_image_base",
        # nonroot-amd64
        digest = "sha256:781e3acb7934ce0fa5ceeb62ee1369248ab23ae26dce002138b3d0e5338d7486",
        image = "gcr.io/distroless/java11-debian11",
    )

    oci_pull(
        name = "java_debug_image_base",
        # debug-nonroot-amd64
        digest = "sha256:95749ff107e1a1e14a62709f305208973530764cdac02a0f7762295dba2c40d2",
        image = "gcr.io/distroless/java11-debian11",
    )

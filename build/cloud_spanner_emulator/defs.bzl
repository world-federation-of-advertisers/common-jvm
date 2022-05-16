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

"""Repository rules/macros for Google Cloud Spanner Emulator."""

load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def _cloud_spanner_emulator_impl(rctx):
    version = rctx.attr.version
    sha256 = rctx.attr.sha256

    url = "https://storage.googleapis.com/cloud-spanner-emulator/releases/{version}/cloud-spanner-emulator_linux_amd64-{version}.tar.gz".format(version = version)

    rctx.download_and_extract(
        url = url,
        sha256 = sha256,
    )
    rctx.template(
        "BUILD.bazel",
        Label("@wfa_common_jvm//build/cloud_spanner_emulator:BUILD.external"),
        executable = False,
    )

_cloud_spanner_emulator_binaries = repository_rule(
    implementation = _cloud_spanner_emulator_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sha256": attr.string(),
    },
)

def cloud_spanner_emulator_binaries():
    maybe(
        _cloud_spanner_emulator_binaries,
        name = "cloud_spanner_emulator",
        sha256 = "7048efb8708e0b2af98d84c9bd86ecb158b5663116c5dea789974d6a616f0e12",
        version = "1.4.2",
    )

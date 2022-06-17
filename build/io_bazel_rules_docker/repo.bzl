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

"""Repository rules/macros for rules_docker."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

_RELEASE_URL = "https://github.com/bazelbuild/rules_docker/releases/download/v{version}/rules_docker-v{version}.tar.gz"
_ARCHIVE_URL = "https://github.com/bazelbuild/rules_docker/archive/{commit}.tar.gz"

def _rules_docker_repo(sha256, version = None, commit = None, name = "io_bazel_rules_docker"):
    """Repository rule for rules_docker.

    Args:
        name: Target name.
        version: Release version, e.g. "0.14.4". Mutually exclusive with commit.
        commit: Commit hash. Mutually exclusive with version.
        sha256: SHA256 hash of the source.

    See https://github.com/bazelbuild/rules_docker
    """
    if version:
        suffix = version
        url = _RELEASE_URL.format(version = version)
    else:
        suffix = commit
        url = _ARCHIVE_URL.format(commit = commit)

    maybe(
        http_archive,
        name = name,
        sha256 = sha256,
        strip_prefix = "rules_docker-" + suffix,
        urls = [url],
    )

def io_bazel_rules_docker():
    _rules_docker_repo(
        name = "io_bazel_rules_docker",
        commit = "f929d80c5a4363994968248d87a892b1c2ef61d4",
        sha256 = "efda18e39a63ee3c1b187b1349f61c48c31322bf84227d319b5dece994380bb6",
    )

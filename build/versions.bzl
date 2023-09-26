# Copyright 2022 The Cross-Media Measurement Authors
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

"""Version information for common dependencies."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

VersionedArchiveInfo = provider(
    "Versioned dependency archive",
    fields = {
        "version": "Version number associated with archive",
        "sha256": "SHA-256 of archive",
        "url_templates": "List of templates of URLs that can be filled in with a version",
        "prefix_template": "Template of prefix to strip from archive",
    },
)

def _format_url_templates(versioned_archive):
    """Returns URL templates with version substituted."""
    return [
        template.format(version = versioned_archive.version)
        for template in versioned_archive.url_templates
    ]

def _format_prefix(versioned_archive):
    if not hasattr(versioned_archive, "prefix_template"):
        return None
    return versioned_archive.prefix_template.format(
        version = versioned_archive.version,
    )

def versioned_http_archive(versioned_archive, name):
    maybe(
        http_archive,
        name = name,
        sha256 = versioned_archive.sha256,
        strip_prefix = _format_prefix(versioned_archive),
        urls = _format_url_templates(versioned_archive),
    )

PROTOBUF = VersionedArchiveInfo(
    version = "21.12",
    sha256 = "22fdaf641b31655d4b2297f9981fa5203b2866f8332d3c6333f6b0107bb320de",
    url_templates = [
        "https://github.com/protocolbuffers/protobuf/archive/v{version}.tar.gz",
    ],
    prefix_template = "protobuf-{version}",
)

PROTOBUF_JAVA_VERSION = "3." + PROTOBUF.version
PROTOBUF_KOTLIN_VERSION = PROTOBUF_JAVA_VERSION

GRPC_JAVA = VersionedArchiveInfo(
    version = "1.52.1",
    sha256 = "6bbe6dca6e60bb892fec8000ab2a200c474c4d9700f34e3fa205c84aaeaf33f7",
    url_templates = [
        "https://github.com/grpc/grpc-java/archive/refs/tags/v{version}.tar.gz",
    ],
    prefix_template = "grpc-java-{version}",
)
GRPC_KOTLIN = VersionedArchiveInfo(
    version = "1.3.0",
    sha256 = "466d33303aac7e825822b402efa3dcfddd68e6f566ed79443634180bb75eab6e",
    url_templates = [
        "https://github.com/grpc/grpc-kotlin/archive/refs/tags/v{version}.tar.gz",
    ],
    prefix_template = "grpc-kotlin-{version}",
)

RULES_JVM_EXTERNAL = VersionedArchiveInfo(
    version = "5.2",
    sha256 = "f86fd42a809e1871ca0aabe89db0d440451219c3ce46c58da240c7dcdc00125f",
    url_templates = [
        "https://github.com/bazelbuild/rules_jvm_external/releases/download/{version}/rules_jvm_external-{version}.tar.gz",
    ],
    prefix_template = "rules_jvm_external-{version}",
)

SPANNER_EMULATOR = VersionedArchiveInfo(
    version = "1.4.9",
    sha256 = "0716bf95e740328cdaef7a7e41e022037fde803596378a9db81b56bc0de1dcb9",
    url_templates = [
        "https://storage.googleapis.com/cloud-spanner-emulator/releases/{version}/cloud-spanner-emulator_linux_amd64-{version}.tar.gz",
    ],
)

RULES_DOCKER = VersionedArchiveInfo(
    version = "0.25.0",
    sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
    url_templates = [
        "https://github.com/bazelbuild/rules_docker/releases/download/v{version}/rules_docker-v{version}.tar.gz",
    ],
)

RULES_KOTLIN = VersionedArchiveInfo(
    version = "1.8.1",
    sha256 = "a630cda9fdb4f56cf2dc20a4bf873765c41cf00e9379e8d59cd07b24730f4fde",
    url_templates = [
        "https://github.com/bazelbuild/rules_kotlin/releases/download/v{version}/rules_kotlin_release.tgz",
    ],
)

KOTLIN_LANGUAGE_LEVEL = "1.8"

# Kotlin release version.
#
# See https://kotlinlang.org/docs/releases.html#release-details.
KOTLIN_RELEASE_VERSION = "1.8.21"

# kotlinx.coroutines version.
KOTLINX_COROUTINES_VERSION = "1.7.3"

# Tink commit that is newer than v1.6.1.
#
# TODO: Use version once there's a release that contains AesSivBoringSsl.
TINK_COMMIT = "0f65dc5d079fb3107c71908734a082079e98ae45"

AWS_JAVA_SDK_VERSION = "2.17.258"

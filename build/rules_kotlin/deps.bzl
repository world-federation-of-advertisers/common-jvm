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

"""Repository rules/macros for rules_kotlin dependencies."""

load(
    "@io_bazel_rules_kotlin//kotlin:repositories.bzl",
    "kotlin_repositories",
    "kotlinc_version",
)
load(
    "//build:versions.bzl",
    "JETBRAINS_ANNOTATIONS_VERSION",
    "KOTLIN_RELEASE_VERSION",
)

def rules_kotlin_deps():
    compiler_release = kotlinc_version(
        release = KOTLIN_RELEASE_VERSION,
        sha256 = "632166fed89f3f430482f5aa07f2e20b923b72ef688c8f5a7df3aa1502c6d8ba",
    )
    kotlin_repositories(
        compiler_release = compiler_release,
    )

    # Override the Kotlin compiler repo with one that has Maven coordinate tags.
    #
    # TODO(bazelbuild/rules_kotlin#752): Drop once compiler repo deps are
    # tagged with Maven coordinates.
    _kotlin_compiler_repo(
        name = "com_github_jetbrains_kotlin",
        urls = [
            url.format(version = compiler_release.version)
            for url in compiler_release.url_templates
        ],
        sha256 = compiler_release.sha256,
    )

    native.register_toolchains(
        "@wfa_common_jvm//build/rules_kotlin/toolchain:toolchain",
    )

def _kotlin_compiler_repo_impl(repository_ctx):
    attr = repository_ctx.attr
    repository_ctx.download_and_extract(
        attr.urls,
        sha256 = attr.sha256,
        stripPrefix = "kotlinc",
    )
    repository_ctx.file(
        "WORKSPACE",
        content = "workspace(name = {name})".format(name = attr.name),
    )
    repository_ctx.template(
        "BUILD.bazel",
        attr._build_template,
        substitutions = {
            "{{kotlin_release_version}}": KOTLIN_RELEASE_VERSION,
            "{{jetbrains_annotations_version}}": JETBRAINS_ANNOTATIONS_VERSION,
        },
        executable = False,
    )

_kotlin_compiler_repo = repository_rule(
    implementation = _kotlin_compiler_repo_impl,
    attrs = {
        "urls": attr.string_list(mandatory = True),
        "sha256": attr.string(),
        "_build_template": attr.label(
            default = ":BUILD.com_github_jetbrains_kotlin",
        ),
    },
)

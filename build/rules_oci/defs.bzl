# Copyright 2023 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Defs for rules_oci."""

load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@rules_multirun//:defs.bzl", "command", "multirun")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push_rule")
load("@rules_pkg//pkg:mappings.bzl", "pkg_files")
load("@rules_pkg//pkg:providers.bzl", "PackageFilesInfo")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//build:defs.bzl", "expand_template", "java_native_libraries")

DEFAULT_JAVA_IMAGE_BASE = Label("@java_image_base")
DEFAULT_JAVA_DEBUG_IMAGE_BASE = Label("@java_debug_image_base")

def _get_repo_mapping_manifest(default_info):
    files_to_run = default_info.files_to_run
    if not files_to_run:
        return None
    return files_to_run.repo_mapping_manifest

def _merge_runfiles(default_infos):
    """Returns a depset of runfiles from the specified DefaultInfo objects."""
    files = [
        default_info.default_runfiles.files
        for default_info in default_infos
        if default_info.default_runfiles != None
    ]

    return depset(transitive = files)

def _runfiles_pkg_impl(ctx):
    include_repo_mapping_manifest = ctx.attr.include_repo_mapping_manifest
    if include_repo_mapping_manifest and len(ctx.attr.deps) != 1:
        fail("Expected exactly one dep when include_repo_mapping_manifest == True")
    excluded_file_extensions = ctx.attr.excluded_file_extensions

    default_infos = [dep[DefaultInfo] for dep in ctx.attr.deps]
    unfiltered_runfiles_depset = _merge_runfiles(default_infos)

    runfiles_depset = depset([
        file
        for file in unfiltered_runfiles_depset.to_list()
        if file not in ctx.files.excluded_files and file.extension not in excluded_file_extensions
    ])
    repo_mapping_manifest = (
        _get_repo_mapping_manifest(default_infos[0]) if include_repo_mapping_manifest else None
    )

    prefix = "/".join([ctx.attr.name, ctx.workspace_name])
    dest_src_map = {
        "/".join([prefix, file.short_path]): file
        for file in runfiles_depset.to_list()
    }
    if repo_mapping_manifest:
        dest_src_map["/".join([ctx.attr.name, "_repo_mapping"])] = repo_mapping_manifest

    all_files = depset(
        direct = [repo_mapping_manifest],
        transitive = [runfiles_depset],
    ) if repo_mapping_manifest else runfiles_depset

    return [
        PackageFilesInfo(dest_src_map = dest_src_map),
        DefaultInfo(files = all_files),
    ]

_runfiles_pkg = rule(
    doc = "Merged runfiles as files",
    implementation = _runfiles_pkg_impl,
    attrs = {
        "deps": attr.label_list(
            providers = [DefaultInfo],
            mandatory = True,
        ),
        "include_repo_mapping_manifest": attr.bool(
            default = False,
        ),
        "excluded_files": attr.label_list(),
        "excluded_file_extensions": attr.string_list(),
    },
)

def java_image(
        name,
        binary,
        base = None,
        labels = None,
        cmd_args = None,
        env = None,
        visibility = None,
        **kwargs):
    """Java container image.

    For standard attributes, see
    https://bazel.build/reference/be/common-definitions

    Args:
      name: name of the resulting oci_image target
      binary: label of java_binary target
      base: label of base Java oci_image
      cmd_args: list of command-line arguments
      env: dictionary of environment variables
      labels: dictionary of labels for the image config
      visibility: standard attribute
      **kwargs: other args to pass to the resulting target
    """
    env = env or {}
    binary_label = native.package_relative_label(binary)
    deploy_jar_file_name = binary_label.name + "_deploy.jar"
    deploy_jar_label = binary_label.relative(deploy_jar_file_name)
    cmd_args = cmd_args or []

    jar_files_name = "{name}_jar_files".format(name = name)
    pkg_files(
        name = jar_files_name,
        srcs = [deploy_jar_label],
        renames = {
            deploy_jar_label: deploy_jar_file_name,
        },
        visibility = ["//visibility:private"],
        **kwargs
    )

    native_libraries_name = "{name}_native_libraries".format(name = name)
    java_native_libraries(
        name = native_libraries_name,
        java_target = binary,
        visibility = ["//visibility:private"],
        **kwargs
    )

    native_library_files_name = "{name}_native_library_files".format(name = name)
    pkg_files(
        name = native_library_files_name,
        srcs = [":" + native_libraries_name],
        prefix = "native",
        visibility = ["//visibility:private"],
        **kwargs
    )

    runfiles_pkg_name = "{name}_runfiles_pkg".format(name = name)
    _runfiles_pkg(
        name = runfiles_pkg_name,
        deps = [binary],
        include_repo_mapping_manifest = True,
        excluded_files = ["@bazel_tools//tools/jdk:current_java_runtime"],
        excluded_file_extensions = ["jar"],
        visibility = ["//visibility:private"],
        **kwargs
    )

    layer_name = "{name}_layer".format(name = name)
    pkg_tar(
        name = layer_name,
        srcs = [
            ":" + jar_files_name,
            ":" + native_library_files_name,
            ":" + runfiles_pkg_name,
        ],
        visibility = ["//visibility:private"],
        **kwargs
    )

    oci_image(
        name = name,
        base = base or DEFAULT_JAVA_IMAGE_BASE,
        tars = [":" + layer_name],
        entrypoint = [
            "java",
            "-Djava.library.path=/native",
            "-jar",
            "/" + deploy_jar_file_name,
        ] + cmd_args,
        labels = labels,
        env = dicts.add(env, RUNFILES_DIR = "/" + runfiles_pkg_name),
        visibility = visibility,
        **kwargs
    )

def container_push(
        name,
        image,
        registry,
        repository,
        tag,
        visibility = None,
        **kwargs):
    """Compatibility macro for container_push using rules_oci.

    For standard attributes, see
    https://bazel.build/reference/be/common-definitions

    Args:
      name: name of the resulting oci_push_rule target
      image: label of oci_image to push
      registry: container registry
      repository: image repository
      tag: image tag
      visibility: standard attribute
      **kwargs: other args to pass to the resulting target
    """

    # Handle "Make" variable substitution
    gen_repositories_name = "_{name}_gen_repositories".format(name = name)
    repositories_name = "_{name}_repositories.txt".format(name = name)
    gen_tags_name = "_{name}_gen_tags".format(name = name)
    tags_name = "_{name}_tags.txt".format(name = name)
    expand_template(
        name = gen_repositories_name,
        template = Label("//build/rules_oci:repositories_tmpl.txt"),
        out = repositories_name,
        visibility = ["//visibility:private"],
        substitutions = {
            "{registry}": registry,
            "{repository}": repository,
        },
        **kwargs
    )
    expand_template(
        name = gen_tags_name,
        template = Label("//build/rules_oci:tags_tmpl.txt"),
        out = tags_name,
        visibility = ["//visibility:private"],
        substitutions = {
            "{tag}": tag,
        },
        **kwargs
    )

    oci_push_rule(
        name = name,
        image = image,
        repository_file = ":" + repositories_name,
        remote_tags = ":" + tags_name,
        visibility = visibility,
        **kwargs
    )

def container_push_all(
        name,
        images,
        visibility = None,
        **kwargs):
    """Convenience macro to push multiple images.

    This is intended as a replacement for container_bundle and docker_push from
    rules_docker.

    Args:
      name: a name for the target
      images: dictionary of image repository URL to oci_image target
      visibility: standard visibility attribute
      **kwargs: other args to pass to the resulting target
    """

    # TODO(bazel-contrib/rules_oci#248): Use a more efficient solution once
    # available.
    for i, (repository_url, image) in enumerate(images.items()):
        (registry, tagged_repo) = repository_url.split("/", 1)
        (repository, tag) = tagged_repo.rsplit(":", 1)
        push_name = "{name}_push_{index}".format(name = name, index = i)
        push_cmd_name = "{name}_push_cmd_{index}".format(name = name, index = i)

        container_push(
            name = push_name,
            image = image,
            registry = registry,
            repository = repository,
            tag = tag,
            visibility = ["//visibility:private"],
            **kwargs
        )

        command(
            name = push_cmd_name,
            command = ":" + push_name,
            visibility = ["//visibility:private"],
            **kwargs
        )

    multirun(
        name = name,
        commands = [
            "{name}_push_cmd_{index}".format(name = name, index = i)
            for i in range(len(images))
        ],
        visibility = visibility,
        **kwargs
    )

java_native_libraries = _java_native_libraries

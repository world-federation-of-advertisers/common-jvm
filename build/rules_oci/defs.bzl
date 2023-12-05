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

load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push_rule")
load("@rules_pkg//pkg:mappings.bzl", "pkg_files")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//build:defs.bzl", "expand_template", "to_label")

DEFAULT_JAVA_IMAGE_BASE = Label("@java_image_base")

def java_image(
        name,
        deploy_jar,
        base = DEFAULT_JAVA_IMAGE_BASE,
        labels = None,
        visibility = None,
        **kwargs):
    """Java container image.

    For standard attributes, see
    https://bazel.build/reference/be/common-definitions

    Args:
      name: name of the resulting oci_image target
      deploy_jar: label of deploy JAR from java_binary
      base: label of base Java oci_image
      labels: dictionary of labels for the image config
      visibility: standard attribute
      **kwargs: other args to pass to the resulting target
    """
    deploy_jar_label = to_label(deploy_jar)

    jar_files_name = "_{name}_jar_files".format(name = name)
    pkg_files(
        name = jar_files_name,
        srcs = [deploy_jar_label],
        renames = {
            deploy_jar_label: "app.jar",
        },
        visibility = ["//visibility:private"],
        **kwargs
    )

    layer_name = "_{name}_layer".format(name = name)
    pkg_tar(
        name = layer_name,
        srcs = [":" + jar_files_name],
        visibility = ["//visibility:private"],
        **kwargs
    )

    oci_image(
        name = name,
        base = base,
        tars = [":" + layer_name],
        entrypoint = [
            "java",
            "-jar",
            "/app.jar",
        ],
        visibility = visibility,
        labels = labels,
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

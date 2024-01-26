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

"""Utility functions definitions."""

# buildifier: disable=unnamed-macro
def to_label(label_string):
    """Returns a Label object for a possibly relative label string."""
    return native.package_relative_label(label_string)

# buildifier: disable=unnamed-macro
def test_target(target):
    """Returns the label for the corresponding target in the test tree."""
    label = to_label(target)
    test_package = label.package.replace("src/main/", "src/test/", 1)
    return Label("@@{workspace}//{package}:{target_name}".format(
        workspace = label.workspace_name,
        package = test_package,
        target_name = label.name,
    ))

def get_real_short_path(file):
    """Returns the correct short path for a `File`.

    Args:
        file: the `File` to return the short path for.

    Returns:
        The short path for `file`, handling any non-standard path segments if
        it's from external repositories.
    """

    # For some reason, files from other archives have short paths that look like:
    #   ../com_google_protobuf/google/protobuf/descriptor.proto
    short_path = file.short_path
    if short_path.startswith("../"):
        second_slash = short_path.index("/", 3)
        short_path = short_path[second_slash + 1:]

    # Sometimes it has another few prefixes like:
    #   _virtual_imports/any_proto/google/protobuf/any.proto
    #   benchmarks/_virtual_imports/100_msgs_proto/benchmarks/100_msgs.proto
    # We want just google/protobuf/any.proto.
    virtual_imports = "_virtual_imports/"
    if virtual_imports in short_path:
        short_path = short_path.split(virtual_imports)[1].split("/", 1)[1]
    return short_path

def _expand_template_impl(ctx):
    substitutions = {
        key: ctx.expand_make_variables("value", value, {})
        for key, value in ctx.attr.substitutions.items()
    }
    ctx.actions.expand_template(
        template = ctx.file.template,
        output = ctx.outputs.out,
        substitutions = substitutions,
    )

expand_template = rule(
    implementation = _expand_template_impl,
    doc = """Template expansion with "Make" variable substitution.

    This delegates to ctx.actions.expand_template. See that function's documentation for the attributes.
    """,
    attrs = {
        "template": attr.label(
            mandatory = True,
            allow_single_file = True,
        ),
        "substitutions": attr.string_dict(
            mandatory = True,
            doc = "A dictionary mapping strings to their substitutions. The values are subject to \"Make\" variable substitution.",
        ),
        "out": attr.output(
            mandatory = True,
        ),
    },
    output_to_genfiles = True,
)

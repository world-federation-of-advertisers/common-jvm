# Copyright 2021 The Cross-Media Measurement Authors
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

"""
Adds external repos necessary for common-jvm.
"""

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
load(
    "@com_google_googleapis//:repository_rules.bzl",
    "switched_rules_by_language",
)
load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")
load("@rules_oci//oci:dependencies.bzl", "rules_oci_dependencies")
load("@rules_pkg//pkg:deps.bzl", "rules_pkg_dependencies", "rules_pkg_register_toolchains")
load(
    "@rules_proto//proto:repositories.bzl",
    "rules_proto_dependencies",
    "rules_proto_toolchains",
)
load("//build/rules_kotlin:deps.bzl", "rules_kotlin_deps")

# buildifier: disable=unnamed-macro
def common_jvm_deps():
    """Adds transitive dependencies of external repos for common-jvm."""
    rules_pkg_dependencies()
    rules_pkg_register_toolchains()
    rules_proto_dependencies()
    rules_proto_toolchains()
    rules_jvm_external_deps()
    grpc_deps()
    rules_kotlin_deps()
    rules_oci_dependencies()
    switched_rules_by_language(
        name = "com_google_googleapis_imports",
        java = True,
    )

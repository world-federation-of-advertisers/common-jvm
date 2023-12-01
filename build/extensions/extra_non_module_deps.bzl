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

"""Module extension for extra non-module dependencies of common_jvm."""

load(
    "@com_google_googleapis//:repository_rules.bzl",
    "switched_rules_by_language",
)

def _extra_non_module_deps_impl(
        # buildifier: disable=unused-variable
        mctx):
    switched_rules_by_language(
        name = "com_google_googleapis_imports",
        java = True,
    )

extra_non_module_deps = module_extension(
    implementation = _extra_non_module_deps_impl,
)

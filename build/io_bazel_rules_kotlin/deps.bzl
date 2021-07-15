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

load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")

def _kotlinc_release(version, sha256):
    return {
        "urls": [
            "https://github.com/JetBrains/kotlin/releases/download/v{v}/kotlin-compiler-{v}.zip".format(v = version),
        ],
        "sha256": sha256,
    }

def rules_kotlin_deps():
    compiler_release = _kotlinc_release(
        sha256 = "dfef23bb86bd5f36166d4ec1267c8de53b3827c446d54e82322c6b6daad3594c",
        version = "1.4.32",
    )
    kotlin_repositories(compiler_release = compiler_release)

    kt_register_toolchains()

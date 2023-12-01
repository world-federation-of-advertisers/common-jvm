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

"""Common macros."""

load(
    "@wfa_rules_kotlin_jvm//kotlin:defs.bzl",
    _kt_jvm_grpc_proto_library = "kt_jvm_grpc_proto_library",
)

def kt_jvm_grpc_proto_library(name, srcs = None, **kwargs):
    """Forwarding macro for kt_jvm_grpc_proto_library.

    Deprecated:
        Use kt_jvm_grpc_proto_library from @wfa_rules_kotlin_jvm//kotlin:defs.bzl.
    """

    # buildifier: disable=print
    print(
        "{context}: Use kt_jvm_grpc_proto_library from @wfa_rules_kotlin_jvm//kotlin:defs.bzl".format(
            context = native.package_name(),
        ),
    )
    _kt_jvm_grpc_proto_library(name, deps = srcs, **kwargs)

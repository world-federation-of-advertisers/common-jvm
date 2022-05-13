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

GRPC_JAVA_VERSION = "1.46.0"
PROTOBUF_VERSION = "3.19.1"

# Kotlin release version.
#
# This is currently the default compiler release version from rules_kotlin.
#
# TODO(world-federation-of-advertisers/common-jvm#116): Reference this directly
# from rules_kotlin once it's available (rules_kotlin v1.5.0+), or explicitly
# specify this version via kotlin_repositories.
KOTLIN_RELEASE_VERSION = "1.4.20"

# kotlinx.coroutines version.
#
# This should be compatible with KOTLIN_RELEASE_VERSION.
# See https://kotlinlang.org/docs/releases.html#release-details.
KOTLINX_COROUTINES_VERSION = "1.4.1"

# Tink commit that is newer than v1.6.1.
#
# TODO: Use version once there's a release that contains AesSivBoringSsl.
TINK_COMMIT = "0f65dc5d079fb3107c71908734a082079e98ae45"

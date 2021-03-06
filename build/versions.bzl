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
GRPC_KOTLIN_VERSION = "1.3.0"
PROTOBUF_VERSION = "3.20.1"
KOTLIN_LANGUAGE_LEVEL = "1.5"

# Kotlin release version.
#
# See https://kotlinlang.org/docs/releases.html#release-details.
KOTLIN_RELEASE_VERSION = "1.6.21"

# Version of org.jetbrains:annotations that comes bundled with
# KOTLIN_RELEASE_VERSION.
JETBRAINS_ANNOTATIONS_VERSION = "13.0"

# kotlinx.coroutines version.
KOTLINX_COROUTINES_VERSION = "1.6.2"

# Tink commit that is newer than v1.6.1.
#
# TODO: Use version once there's a release that contains AesSivBoringSsl.
TINK_COMMIT = "0f65dc5d079fb3107c71908734a082079e98ae45"

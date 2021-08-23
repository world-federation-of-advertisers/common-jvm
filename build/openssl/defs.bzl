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
Build defs for calling the OpenSSL CLI.
"""

load("@bazel_skylib//rules:write_file.bzl", "write_file")

_SKI_EXT = "subjectKeyIdentifier=hash"
_AKI_EXT = "authorityKeyIdentifier=keyid:always,issuer"
_BASIC_CONSTRAINTS_EXT = "basicConstraints=CA:FALSE"
_KEY_USAGE_EXT = "keyUsage=nonRepudiation,digitalSignature,keyEncipherment"

def _location(label):
    return "$(location {label})".format(label = label)

def _subject(org, common_name):
    return "'/O={org}/CN={common_name}'".format(
        org = org,
        common_name = common_name,
    )

def _subject_alt_name(hostname):
    return "subjectAltName=DNS:{hostname}".format(hostname = hostname)

def generate_root_certificate(
        name,
        org,
        common_name,
        hostname,
        valid_days = 3650,
        cache_version = 0,
        visibility = None):
    """Generates a root certificate and private key.

    Uses OpenSSL using elliptic curve crypto over the P-256 curve.

    Args:
        name: Target name. The outputs will be *name*.pem and *name*.key.
        org: Subject organization name.
        common_name: Subject common name.
        hostname: DNS hostname for Subject Alternative Name extension.
        valid_days: How many days the certificate is valid for.
        cache_version: The Bazel build cache version of this target. This can be
            used to invalidate any cached cert, e.g. if it has expired.
        visibility: The visibility of the generated target.
    """
    cert = name + ".pem"
    key = name + ".key"
    cmd = [
        "openssl",
        "req",
        "-out",
        _location(cert),
        "-new",
        "-newkey ec",
        "-pkeyopt ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout",
        _location(key),
        "-x509",
        "-days",
        str(valid_days),
        "-subj",
        _subject(org = org, common_name = common_name),
        "-extensions v3_ca",
        "-addext",
        _subject_alt_name(hostname = hostname),
        "# cache_version:" + str(cache_version),
    ]
    native.genrule(
        name = "gen_" + name,
        outs = [
            cert,
            key,
        ],
        cmd = " ".join(cmd),
        visibility = ["//visibility:private"],
        message = "Generating root certificate and key",
        testonly = True,
    )

    native.filegroup(
        name = name,
        srcs = [cert, key],
        visibility = visibility,
        testonly = True,
    )

def generate_user_certificate(
        name,
        root_key,
        root_certificate,
        org,
        common_name,
        hostname,
        valid_days = 365,
        cache_version = 0,
        visibility = None):
    """Generates a private key and a certificate that is signed by the root authority.

    Uses OpenSSL using elliptic curve crypto over the P-256 curve.

    Args:
        name: Target name. The outputs will be *name*.pem and *name*.key.
        root_key: Private key of root CA.
        root_certificate: Certificate of root CA in PEM format.
        org: Subject organization name.
        common_name: Subject common name.
        hostname: DNS hostname for Subject Alternative Name extension.
        valid_days: How many days the certificate is valid for.
        cache_version: The Bazel build cache version of this target. This can be
            used to invalidate any cached cert, e.g. if it has expired.
        visibility: The visibility of the generated target.
    """
    cert = name + ".pem"
    key = name + ".key"
    csr = name + ".csr"
    v3_config = name + ".cnf"

    san_ext = _subject_alt_name(hostname = hostname)

    csr_cmd = [
        "openssl",
        "req",
        "-out",
        _location(csr),
        "-new",
        "-newkey ec",
        "-pkeyopt ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout",
        _location(key),
        "-subj",
        _subject(org = org, common_name = common_name),
    ]
    native.genrule(
        name = name + "_csr",
        outs = [csr, key],
        cmd = " ".join(csr_cmd),
        visibility = ["//visibility:private"],
        message = "Generating user certificate signing request and key",
        testonly = True,
    )

    write_file(
        name = name + "_v3_config",
        out = v3_config,
        content = [
            "[usr_cert]",
            _BASIC_CONSTRAINTS_EXT,
            _AKI_EXT,
            _SKI_EXT,
            _KEY_USAGE_EXT,
            san_ext,
        ],
        visibility = ["//visibility:private"],
        testonly = True,
    )

    cmd = [
        "openssl",
        "x509",
        "-in",
        _location(csr),
        "-out",
        _location(cert),
        "-days",
        str(valid_days),
        "-req",
        "-CA",
        _location(root_certificate),
        "-CAkey",
        _location(root_key),
        "-CAcreateserial",
        "-extfile",
        _location(v3_config),
        "-extensions usr_cert",
        "# cache_version:" + str(cache_version),
    ]
    native.genrule(
        name = "gen_" + name,
        outs = [cert],
        srcs = [root_key, root_certificate, csr, v3_config],
        cmd = " ".join(cmd),
        visibility = ["//visibility:private"],
        message = "Generating user certificate",
        testonly = True,
    )

    native.filegroup(
        name = name,
        srcs = [cert, key],
        visibility = visibility,
        testonly = True,
    )

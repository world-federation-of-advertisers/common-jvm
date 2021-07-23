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
This module contains genrules to generate certificates for different parties.
"""

def generate_root_certificate(
        name,
        org = "Server",
        common_name = "ca.server.example.com",
        visibility = None,
        ssl_conf = "@wfa_common_jvm//build/wfa:openssl.cnf"):
    """Generates a root certificate and private key.

    Uses OpenSSL using elliptic curve crypto over the P-256 curve.

    Args:
        name: the entity owning the certificate
        org: org name on the ceritificate
        common_name: common name on the ceritificate
        ssl_conf: the ssl configuration used by OpenSSL
        visibility: the bazel visibility of the generated rule
    """
    cmd_args = [
        "-out $(RULEDIR)/{}.pem".format(name),
        "-new",
        "-newkey ec",
        "-pkeyopt ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout $(RULEDIR)/{}.key".format(name),
        "-x509",
        "-days 365",
        "-subj '/O={} CA/CN={}'".format(org, common_name),
        "-config $(location {})".format(ssl_conf),
        "-extensions v3_ca",
    ]
    native.genrule(
        name = name,
        srcs = [ssl_conf],
        outs = [
            name + ".pem",
            name + ".key",
        ],
        cmd = ("openssl req " + " ".join(cmd_args)),
        visibility = visibility,
    )

def generate_user_certificate(
        name,
        root_key,
        root_certificate,
        org = "Server",
        common_name = "server.example.com",
        visibility = None,
        ssl_conf = "@wfa_common_jvm//build/wfa:openssl.cnf"):
    """Generates a private key and a certificate that is signed by the root authority.

    Uses OpenSSL using elliptic curve crypto over the P-256 curve.

    Args:
        name: the entity owning the certificate
        root_key: private key of root ca
        root_certificate: pem of root ca
        org: org name on the ceritificate
        common_name: common name on the ceritificate
        visibility: the bazel visibility of the generated rule
        ssl_conf: the ssl configuration used by OpenSSL
    """
    cmd1_args = [
        "-out $(RULEDIR)/{}.csr".format(name),
        "-new",
        "-newkey",
        "ec",
        "-pkeyopt ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout $(RULEDIR)/{}.key".format(name),
        "-subj '/O={}/CN={}'".format(org, common_name),
        "-config $(location {})".format(ssl_conf),
        "-extensions v3_req",
    ]
    cmd2_args = [
        "-in $(RULEDIR)/{}.csr".format(name),
        "-out $(RULEDIR)/{}.pem".format(name),
        "-days 365",
        "-req",
        "-CA $(RULEDIR)/{}".format(root_certificate.split(":")[1]),
        "-CAkey $(RULEDIR)/{}".format(root_key.split(":")[1]),
        "-CAcreateserial",
        "-extfile $(location {})".format(ssl_conf),
        "-extensions usr_cert",
    ]
    native.genrule(
        name = name,
        srcs = [root_key] + [root_certificate] + [ssl_conf],
        outs = [
            name + ".key",
            name + ".pem",
        ],
        cmd = ("openssl req " + " ".join(cmd1_args) + " && openssl x509 " + " ".join(cmd2_args)),
        visibility = visibility,
    )

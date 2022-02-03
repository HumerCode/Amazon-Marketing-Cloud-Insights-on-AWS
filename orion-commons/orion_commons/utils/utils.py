#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from aws_cdk.aws_ssm import StringParameter
from aws_cdk.core import Construct, RemovalPolicy

from .. import exceptions


def get_removal_policy(value: str) -> RemovalPolicy:
    if value == "destroy":
        return RemovalPolicy.DESTROY
    elif value == "retain":
        return RemovalPolicy.RETAIN
    elif value == "snapshot":
        return RemovalPolicy.SNAPSHOT
    else:
        raise exceptions.RemovalPolicyNotFound


def get_ssm_value(scope: Construct, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from aws_cdk.aws_s3 import Bucket
from aws_cdk.core import RemovalPolicy, Stack
from orion_commons import S3Factory


def test_get_bucket(test_stack: Stack) -> None:
    _fn = S3Factory.bucket(
        scope=test_stack,
        environment_id="dev",
        id="test-bucket",
        bucket_name="test-bucket",
        versioned=True,
        removal_policy=RemovalPolicy.DESTROY,
    )

    assert isinstance(_fn, Bucket)

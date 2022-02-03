#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from aws_cdk.aws_kms import Key
from aws_cdk.core import Stack
from orion_commons import KMSFactory


def test_get_key(test_stack: Stack) -> None:
    _key = KMSFactory.key(
        scope=test_stack,
        environment_id="dev",
        id="test-key",
        description="Test KMS Key",
    )

    assert isinstance(_key, Key)

#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import pytest
from aws_cdk.core import RemovalPolicy, Stack
from orion_commons import RemovalPolicyNotFound
from orion_commons.utils import get_removal_policy


def test_get_removal_policy(test_stack: Stack) -> None:
    assert get_removal_policy("destroy") == RemovalPolicy.DESTROY
    assert get_removal_policy("retain") == RemovalPolicy.RETAIN
    assert get_removal_policy("snapshot") == RemovalPolicy.SNAPSHOT
    with pytest.raises(RemovalPolicyNotFound):
        get_removal_policy("dummy")


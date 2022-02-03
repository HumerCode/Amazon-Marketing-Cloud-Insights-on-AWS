#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from pathlib import Path

from aws_cdk.aws_lambda import Code, Function
from aws_cdk.core import Stack
from orion_commons import LambdaFactory


def test_get_lambda(test_stack: Stack) -> None:
    _fn = LambdaFactory.function(
        scope=test_stack,
        environment_id="dev",
        id="test-lambda",
        code=Code.from_asset(f"{Path(__file__).parents[2]}"),
        handler="orion_commons.handlers.lambda_handler",
    )

    assert isinstance(_fn, Function)

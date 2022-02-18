#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import json
from typing import Any
from unittest.mock import mock_open, patch

from aws_cdk.core import App, Construct, Stack
from orion_commons import CDKContextConfigStrategy, Config
from pytest import fixture

context_json = {
    "cdk_version": "1.108.0",
    "environments": {
        "cicd": {
            "account": "241578417185",
            "region": "us-east-1",
            "execute_security_lint": True,
        },
        "dev": {
            "account": "123456789012",
            "region": "us-east-1",
            "name": "Development",
            "execute_tests": True,
            "manual_approvals": True,
            "resource_config": {
                "test-lambda": {"memory_size": 512},
                "test-bucket": {"versioned": False},
                "test-key": {"enable_key_rotation": True},
                "test-ddbtable": {"removal_policy": "retain"},
                "test-eventbus": {"removal_policy": "retain"},
            },
        },
    },
}


@fixture()
def cdk_app() -> App:
    return App(context=context_json)


@fixture()
def test_stack(cdk_app: App) -> Stack:
    class TestStack(Stack):
        def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
            super().__init__(scope, construct_id, **kwargs)

    return TestStack(cdk_app, "test-stack")


@fixture()
def config_context(test_stack: Stack) -> Config:
    return Config(config_strategy=CDKContextConfigStrategy(test_stack.node))


@fixture()
@patch("builtins.open", mock_open(read_data=json.dumps({"context": context_json})))
def config_json() -> Config:
    return Config()

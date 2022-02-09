#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import json
from typing import Any, Dict
from unittest.mock import mock_open, patch

from aws_cdk.core import App, Construct, Stack
from orion_commons import Config
from pytest import fixture

context_json = {
    "cdk_version": "1.108.0",
    "environments": {
        "dev": {
            "account": "123456789012",
            "region": "us-east-1",
            "name": "Development",
            "resource_config": {
                "test-lambda-transform-dev-lambda": {"memory_size": 512},
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
@patch("builtins.open", mock_open(read_data=json.dumps({"context": context_json})))
def config_json() -> Config:
    return Config()


@fixture()
def custom_resource_event() -> Dict[str, Any]:
    return {
        "PhysicalResourceId": "PhysicalResource",
        "LogicalResourceId": "Resource1",
        "ResourceType": "AWS::CloudFormation::CustomResource",
        "ResourceProperties": {
            "ServiceToken": "arn:aws:lambda:us-east-1:111111111111:function:Test-HMkIocwWu0pF",
            "RegisterProperties": {
                "id": "id",
                "name": "my-fancy-name",
                "description": "",
            },
        },
    }

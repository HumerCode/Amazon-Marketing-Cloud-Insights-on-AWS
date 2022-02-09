# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
from typing import Any, Dict
from unittest.mock import patch

import boto3
import pytest
from moto import mock_dynamodb2

test_env = {"DATASET_TABLE_NAME": "dataset", "PIPELINE_TABLE_NAME": "pipeline", "STAGE_TABLE_NAME": "stage"}


@pytest.fixture(scope="session")
def moto_ddb() -> Any:
    with mock_dynamodb2():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        tables = {}
        for _, name in test_env.items():
            tables[name] = ddb.create_table(
                TableName=name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            )
        yield tables


@patch.dict(os.environ, test_env)
@pytest.mark.parametrize("type", ["dataset", "pipeline", "stage"])
@pytest.mark.parametrize("event_type", ["Create", "Update", "Delete"])
def test_catalog_lambda(moto_ddb: Any, event_type: str, custom_resource_event: Dict[str, Any], type: str) -> None:
    from orion_satellite.foundations.lambdas.register.handler import on_event

    custom_resource_event["RequestType"] = event_type
    custom_resource_event["ResourceProperties"]["RegisterProperties"]["type"] = type
    expected_response = custom_resource_event["ResourceProperties"]["RegisterProperties"]
    on_event(custom_resource_event, None)
    response = moto_ddb[type].get_item(Key={"id": expected_response["id"]})
    if event_type == "Delete":
        assert "Item" not in response
    else:
        assert response["Item"] == expected_response

#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import json
import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_events, mock_s3

test_env = {
    "EVENT_SOURCE": "test",
    "EVENT_DETAIL_TYPE": "test",
    "OUTPUT_BUCKET_NAME": "test_out",
}
test_event = {"detail": {"requestParameters": {"bucketName": "test", "key": "test/test.json"}}}


@pytest.fixture(scope="function")
def moto_s3() -> boto3.client:
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test")
        s3.create_bucket(Bucket="test_out")
        yield s3


@pytest.fixture(scope="function")
def moto_events() -> boto3.client:
    with mock_events():
        ev = boto3.client("events", region_name="us-east-1")
        yield ev


@patch.dict(os.environ, test_env)
def test_lambda_transform_handler_simple(moto_s3: boto3.client, moto_events: boto3.client) -> None:
    from orion_satellite.producer.lambdas.legislators_lambda_transform.handler import lambda_handler

    moto_s3.put_object(Bucket="test", Key="test/test.json", Body=json.dumps([{"c": [1, 2, 3]}]))
    lambda_handler(test_event, None)
    assert moto_s3.get_object(Bucket="test_out", Key="test/test_parsed.json")["Body"]

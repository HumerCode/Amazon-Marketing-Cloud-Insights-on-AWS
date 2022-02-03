#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from aws_cdk.aws_dynamodb import Attribute, AttributeType, BillingMode, Table, TableEncryption
from aws_cdk.core import Stack
from orion_commons import DynamoFactory


def test_get_ddbtable(test_stack: Stack) -> None:
    _tbl = DynamoFactory.table(
        scope=test_stack,
        environment_id="dev",
        id="test-ddbtable",
        table_name="test-ddbtable",
        billing_mode=BillingMode.PAY_PER_REQUEST,
        encryption=TableEncryption.DEFAULT,
        partition_key=Attribute(name="test-part-key", type=AttributeType.STRING),
        sort_key=Attribute(name="test-sort-key", type=AttributeType.STRING),
    )

    assert isinstance(_tbl, Table)

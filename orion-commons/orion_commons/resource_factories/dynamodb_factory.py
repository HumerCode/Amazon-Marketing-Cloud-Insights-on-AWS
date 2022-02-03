#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any, Dict, Optional

from aws_cdk.aws_dynamodb import Attribute, BillingMode, Table, TableEncryption
from aws_cdk.core import Construct, RemovalPolicy

from ..config import Config
from ..utils import get_removal_policy

_logger: logging.Logger = logging.getLogger(__name__)


class DynamoFactory:
    @staticmethod
    def table(
        scope: Construct,
        environment_id: str,
        id: str,
        partition_key: Attribute,
        table_name: Optional[str] = None,
        sort_key: Optional[Attribute] = None,
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        encryption: TableEncryption = TableEncryption.DEFAULT,
        billing_mode: BillingMode = BillingMode.PAY_PER_REQUEST,
        **table_props: Any,
    ) -> Table:
        config = Config()

        table_config: Dict[str, Any] = config.get_resource_config(id=id, environment_id=environment_id)

        _logger.debug(f"DynamoDB Table config {id}: {table_config}")

        removal_policy_config = table_config.get("removal_policy")
        if removal_policy_config:
            removal_policy = get_removal_policy(removal_policy_config)

        return Table(
            scope,
            id,
            table_name=table_name,
            removal_policy=removal_policy,
            partition_key=partition_key,
            sort_key=sort_key,
            encryption=encryption,
            billing_mode=billing_mode,
            **table_props,
        )

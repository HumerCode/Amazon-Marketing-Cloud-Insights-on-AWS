#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any, Dict

import aws_cdk.aws_s3 as s3
from aws_cdk.core import Construct, RemovalPolicy

from ..config import Config
from ..utils import get_removal_policy

_logger: logging.Logger = logging.getLogger(__name__)


class S3Factory:
    @staticmethod
    def bucket(
        scope: Construct,
        environment_id: str,
        id: str,
        bucket_name: str,
        versioned: bool = False,
        access_control: s3.BucketAccessControl = s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
        block_public_access: s3.BlockPublicAccess = s3.BlockPublicAccess.BLOCK_ALL,
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        encryption: s3.BucketEncryption = s3.BucketEncryption.S3_MANAGED,
        **bucket_props: Any,
    ) -> s3.Bucket:
        config = Config()

        s3_config: Dict[str, Any] = config.get_resource_config(
            id,
            environment_id=environment_id,
        )

        _logger.debug(f"S3 config {id}: {s3_config}")

        removal_policy_config = s3_config.get("removal_policy")
        if removal_policy_config:
            removal_policy = get_removal_policy(removal_policy_config)

        return s3.Bucket(
            scope,
            id,
            bucket_name=bucket_name,
            versioned=s3_config.get("versioned") or versioned,
            access_control=access_control,
            block_public_access=block_public_access,
            removal_policy=removal_policy,
            encryption=encryption,
            **bucket_props,
        )

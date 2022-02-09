#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any

from aws_cdk.aws_kms import Key
from aws_cdk.aws_s3 import Bucket, BucketEncryption, BucketAccessControl, BlockPublicAccess
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.core import Construct, Stack, RemovalPolicy, Duration


class BaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._environment_id: str = environment_id
        self._create_bucket(name="artifacts")
        self._create_bucket(name="athena")

    def _create_bucket(self, name: str) -> None:
        bucket_key: Key = Key(
            self,
            id=f"{name}-bucket-key",
            description=f"Orion {name.title()} Key",
            alias=f"orion-{name}-bucket-key",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.DESTROY,
        )

        StringParameter(
            self,
            f"{name}-bucket-key-arn-ssm",
            parameter_name=f"/Orion/KMS/{name.title()}BucketKeyArn",
            string_value=bucket_key.key_arn,
        )
        bucket: Bucket = Bucket(
            self,
            id=f"{name}-bucket",
            bucket_name=f"orion-{self._environment_id}-{self.region}-{self.account}-{name}",
            encryption=BucketEncryption.KMS,
            encryption_key=bucket_key,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN
        )

        StringParameter(
            self,
            f"{name}-bucket-arn-ssm",
            parameter_name=f"/Orion/S3/{name.title()}BucketArn",
            string_value=bucket.bucket_arn,
        )

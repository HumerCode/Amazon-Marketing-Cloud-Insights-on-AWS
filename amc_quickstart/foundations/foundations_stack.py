# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from pathlib import Path
from typing import Any, Dict

import aws_cdk as cdk
from aws_ddk_core.base import BaseStack
import aws_cdk.aws_dynamodb as DDB
from aws_cdk.aws_cloudtrail import ReadWriteType, S3EventSelector, Trail
from aws_cdk.aws_iam import Effect, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_kms import Key
from aws_cdk.aws_lakeformation import CfnResource
from aws_cdk.aws_lambda import Code, Function, Runtime, LayerVersion
from aws_cdk.aws_s3 import Bucket, BucketEncryption, BlockPublicAccess, BucketAccessControl
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.custom_resources import Provider
from aws_cdk.aws_sqs import Queue, DeadLetterQueue, QueueEncryption
from aws_cdk.aws_lambda import EventSourceMapping
from aws_ddk_core.resources import S3Factory, KMSFactory, LambdaFactory
from aws_cdk.aws_sam import CfnApplication

class FoundationsStack(BaseStack):
    def __init__(
        self,
        scope,
        construct_id: str,
        environment_id: str,
        resource_prefix: str,
        app: str,
        org:str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)

        self._environment_id: str = environment_id
        self._app = app
        self._org = org
        self._resource_prefix = resource_prefix
        
        # CustomerConfig DDB Table
        self._customer_config_table = self._create_customer_config_ddb_table(
            name=f"ats-customer-config-{self._environment_id}",
            ddb_props={"partition_key": DDB.Attribute(name="customer_hash_key", type=DDB.AttributeType.STRING),
                        "sort_key": DDB.Attribute(name="hash_key", type=DDB.AttributeType.STRING)},
        )

        self._object_metadata = self._create_octagon_ddb_table(
            name=f"octagon-ObjectMetadata-{self._environment_id}",
            ddb_props={"partition_key": DDB.Attribute(name="id", type=DDB.AttributeType.STRING)},
        )
    
        self._datasets = self._create_octagon_ddb_table(
            name=f"octagon-Datasets-{self._environment_id}",
            ddb_props={"partition_key": DDB.Attribute(name="name", type=DDB.AttributeType.STRING)},
        )

        self._pipelines = self._create_octagon_ddb_table(
            name=f"octagon-Pipelines-{self._environment_id}",
            ddb_props={"partition_key": DDB.Attribute(name="name", type=DDB.AttributeType.STRING)},
        )
        self._peh = self._create_octagon_ddb_table(
            name=f"octagon-PipelineExecutionHistory-{self._environment_id}",
            ddb_props={"partition_key": DDB.Attribute(name="id", type=DDB.AttributeType.STRING)},
        )

        
        self._create_register()
        self._create_routing_lambda()
        self._create_lakeformation_bucket_registration_role()
        self._raw_bucket = self._create_bucket(name="raw")
        self._stage_bucket = self._create_bucket(name="stage")
        self._analytics_bucket = self._create_bucket(name="analytics")
        self._artifacts_bucket = self._create_bucket(name="artifacts")
        self._athena_bucket = self._create_bucket(name="athena")
        
        self._create_wrangler_layers()
        self._create_data_lake_library_layer()
        
        self._create_trail()

    def _create_routing_lambda(self) -> None:

        #Lambda
        self._routing_function: Function = LambdaFactory.function(
            self,
            id=f"{self._resource_prefix}-data-lake-routing-function",
            environment_id = self._environment_id,
            function_name=f"{self._resource_prefix}-data-lake-routing",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/routing")),
            handler="handler.lambda_handler",
            description="routes to the right team and pipeline",
            timeout=cdk.Duration.seconds(60),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
            environment={
                "ENV": self._environment_id,
                "APP": self._app,
                "ORG": self._org,
                "PREFIX": self._resource_prefix
            },
        )
        
        self._routing_function.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:*"
                    ],
                    resources=[
                        self._customer_config_table.table_arn,
                        f"{self._customer_config_table.table_arn}/*",
                        self._object_metadata.table_arn,
                        f"{self._object_metadata.table_arn}/*",
                        self._datasets.table_arn,
                        f"{self._datasets.table_arn}/*"
                    ],
                )
            )
        self._routing_function.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:*"
                    ],
                    resources=["*"],
                    conditions={
                        "ForAnyValue:StringLike":{
                            "kms:ResourceAliases": f"alias/*"
                        }
                    }
                )
        )
        self._routing_function.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sqs:*"
                    ],
                    resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._resource_prefix}-*"],
                )
        )
        self._routing_function.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters"
                    ],
                    resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/AMC/*"],
                )
        )

        self._routing_function.add_permission(
            id= "invoke-lambda-eventbridge",
            principal= ServicePrincipal("events.amazonaws.com"),
            action= "lambda:InvokeFunction"
        )

        StringParameter(
            self,
            f"{self._resource_prefix}-data-lake-routing-lambda-ssm",
            parameter_name=f"/AMC/Lambda/Routing",
            string_value=self._routing_function.function_arn,
        )
    
    def _create_customer_config_ddb_table(self, name: str, ddb_props: Dict[str, Any]) -> DDB.Table:
        tbleName = "CustomerConfig"

        #ddb kms key resource
        table_key: Key = KMSFactory.key(
            self,
            id=f"{self._resource_prefix}-{name}-table-key",
            environment_id = self._environment_id,
            description=f"{self._resource_prefix} {name.title()} Table Key",
            alias=f"{self._resource_prefix}-{name}-ddb-table-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        #SSM for ddb kms table arn
        StringParameter(
            self,
            f"{self._resource_prefix}-{name}-table-key-arn-ssm",
            parameter_name=f"/AMC/KMS/{tbleName.title()}DDBKeyArn",
            string_value=table_key.key_arn,
        )

        #ddb resource
        table: DDB.Table = DDB.Table(
            self,
            f"{self._resource_prefix}-{name}-table",
            table_name = f"{self._resource_prefix}-{name}",
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=table_key,
            billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
            removal_policy= cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        table.add_global_secondary_index(
            index_name="amc-index",
            partition_key=DDB.Attribute(name="hash_key", type=DDB.AttributeType.STRING)
        )
        
        #SSM for ddb table arn
        StringParameter(
            self,
            f"{self._resource_prefix}-{name}-table-arn-ssm",
            parameter_name=f"/AMC/DynamoDB/ats/{tbleName}",
            string_value=table.table_name,
        )

        return table
    
    def _create_octagon_ddb_table(self, name: str, ddb_props: Dict[str, Any]) -> DDB.Table:
        
        tbleName = name.split("-")[1]

        #ddb kms key resource
        table_key: Key = KMSFactory.key(
            self,
            id=f"{name}-table-key",
            environment_id = self._environment_id,
            description=f"{self._resource_prefix} {name.title()} Table Key",
            alias=f"{self._resource_prefix}-{name}-ddb-table-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        #SSM for ddb kms table arn
        StringParameter(
            self,
            f"{name}-table-key-arn-ssm",
            parameter_name=f"/AMC/KMS/{tbleName.title()}DDBKeyArn",
            string_value=table_key.key_arn,
        )

        #SSM for ddb kms table arn
        StringParameter(
            self,
            f"{name}-table-key-id-ssm",
            parameter_name=f"/AMC/KMS/{tbleName.title()}DDBKeyId",
            string_value=table_key.key_id,
        )

        #ddb resource
        table: DDB.Table = DDB.Table(
            self,
            f"{name}-table",
            table_name=name,
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=table_key,
            billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
            removal_policy= cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        #SSM for ddb table arn
        StringParameter(
            self,
            f"{name}-table-arn-ssm",
            parameter_name=f"/AMC/DynamoDB/{tbleName.title()}Arn",
            string_value=table.table_arn,
        )

        #SSM for ddb table name
        StringParameter(
            self,
            f"{name}-table-name-ssm",
            parameter_name=f"/AMC/DynamoDB/{tbleName}",
            string_value=name,
        )
        return table

    def _create_register(self) -> None:
        self._register_function: Function = LambdaFactory.function(
            self,
            id="register-function",
            environment_id = self._environment_id,
            code=Code.from_asset(os.path.join(f"{Path(__file__).parent}", "lambdas/register")),
            handler="handler.on_event",
            memory_size=256,
            description="Registers Datasets, Pipelines and Stages into their respective DynamoDB tables",
            timeout=cdk.Duration.seconds(15 * 60),
            runtime = Runtime.PYTHON_3_8,
            environment={
                "OCTAGON_DATASET_TABLE_NAME": self._datasets.table_name,
                "OCTAGON_PIPELINE_TABLE_NAME": self._pipelines.table_name
            },
        )
        self._datasets.grant_read_write_data(self._register_function)
        self._pipelines.grant_read_write_data(self._register_function)

        self._register_provider = Provider(
            self,
            "register-provider",
            on_event_handler=self._register_function,
        )
        StringParameter(
            self,
            "register-service-token-ssm",
            parameter_name=f"/AMC/Lambda/RegisterProviderServiceToken",
            string_value=self._register_provider.service_token,
        )

    def _create_lakeformation_bucket_registration_role(self) -> None:
        self.lakeformation_bucket_registration_role: Role = Role(
            self,
            "lakeformation-bucket-registration-role",
            assumed_by=ServicePrincipal("lakeformation.amazonaws.com"),
            inline_policies={
                "LakeFormationDataAccessPolicyForS3": PolicyDocument(
                    statements=[
                        PolicyStatement(
                            effect=Effect.ALLOW,
                            actions=["s3:ListAllMyBuckets"],
                            resources=[
                                self.format_arn(
                                    resource="*",
                                    service="s3",
                                    region="",
                                    account="",
                                ),
                            ],
                        ),
                        PolicyStatement(
                            effect=Effect.ALLOW,
                            actions=["s3:ListBucket"],
                            resources=[
                                self.format_arn(
                                    resource=f"{self._resource_prefix}-{self._environment_id}-{self.region}-{self.account}-*",
                                    service="s3",
                                    region="",
                                    account="",
                                ),
                            ],
                        ),
                        PolicyStatement(
                            effect=Effect.ALLOW,
                            actions=["s3:*Object*"],
                            resources=[
                                self.format_arn(
                                    resource=f"{self._resource_prefix}-{self._environment_id}-{self.region}-{self.account}-*",
                                    service="s3",
                                    resource_name="*",
                                    region="",
                                    account="",
                                ),
                            ],
                        ),
                    ]
                )
            },
        )

    def _create_bucket(self, name: str) -> Bucket:
        bucket_key: Key = KMSFactory.key(
            self,
            id=f"{self._resource_prefix}-{name}-bucket-key",
            environment_id=self._environment_id,
            description=f"{self._resource_prefix} {name.title()} Bucket Key",
            alias=f"{self._resource_prefix}-{name}-bucket-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        StringParameter(
            self,
            f"{self._resource_prefix}-{name}-bucket-key-arn-ssm",
            parameter_name=f"/AMC/KMS/{name.title()}BucketKeyArn",
            string_value=bucket_key.key_arn,
        )

        bucket: Bucket = S3Factory.bucket(
            self,
            id=f"{self._resource_prefix}-{name}-bucket",
            environment_id = self._environment_id,
            bucket_name=f"{self._resource_prefix}-{self._environment_id}-{self.region}-{self.account}-{name}",
            encryption=BucketEncryption.KMS,
            encryption_key=bucket_key,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN
        )

        StringParameter(
            self,
            f"{self._resource_prefix}-{name}-bucket-arn-ssm",
            parameter_name=f"/AMC/S3/{name.title()}BucketArn",
            string_value=bucket.bucket_arn,
        )
        StringParameter(
            self,
            f"{self._resource_prefix}-{name}-bucket-name-ssm",
            parameter_name=f"/AMC/S3/{name.title()}Bucket",
            string_value=f"{self._resource_prefix}-{self._environment_id}-{self.region}-{self.account}-{name}",
        )
        CfnResource(
            self,
            f"{self._resource_prefix}-{name}-bucket-lakeformation-registration",
            resource_arn=bucket.bucket_arn,
            use_service_linked_role=False,
            role_arn=self.lakeformation_bucket_registration_role.role_arn,
        )
        bucket_key.add_to_resource_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey*",
                    "kms:ReEncrypt*",
                ],
                resources=["*"],
                principals=[self.lakeformation_bucket_registration_role],
            )
        )
        return bucket
    
    def _create_wrangler_layers(self) -> None:
            CfnApplication(
                self,
                "wrangler-layers",
                location=CfnApplication.ApplicationLocationProperty(
                    application_id="arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-8",
                    semantic_version="2.12.0",
                ),
            )
    
    def _create_data_lake_library_layer(self) -> None:
        data_lake_library_layer = LayerVersion(
            self,
            "data-lake-library-layer",
            layer_version_name=f"data-lake-library",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "foundations/layers/data_lake_library")),
            compatible_runtimes=[Runtime.PYTHON_3_8],
            description=f"{self._resource_prefix} Data Lake Library",
            license="Apache-2.0",
        )

        StringParameter(
            self,
            f"data_lake_library_layer",
            parameter_name=f"/AMC/Layer/DataLakeLibrary",
            string_value=data_lake_library_layer.layer_version_arn,
        )
        
    def _create_trail(self) -> None:
        Trail(
            self,
            f"{self._resource_prefix} Trail",
            is_multi_region_trail=False,
            include_global_service_events=False,
            management_events=ReadWriteType.ALL,
        ).log_all_s3_data_events()

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

from typing import Any, Dict, List, Optional
import json
from aws_cdk.aws_kms import Key, IKey
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal, AccountRootPrincipal, AnyPrincipal
from aws_ddk_core.base import BaseStack
import aws_cdk as cdk
import aws_cdk.aws_sagemaker as sagemaker
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_s3_deployment import BucketDeployment, ServerSideEncryption, Source
from aws_cdk.aws_ssm import StringParameter
from aws_ddk_core.resources import KMSFactory


def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value
class PlatformManagerSageMaker(BaseStack):
    def __init__(
        self,
        scope,
        construct_id: str,
        environment_id: str,
        microservice: str,
        resource_prefix: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)

        self._environment_id: str = environment_id
        self._microservice_name = microservice
        self._resource_prefix = resource_prefix
        self._get_artifacts()
        self._sync_zip_s3_bucket()
        self._create_sagemaker_kms_key()
        self._create_sagemaker_componenets()

    def _get_artifacts(self) -> None:
        self._artifacts_key: IKey = Key.from_key_arn(
            self,
            "artifacts-bucket-key",
            key_arn=get_ssm_value(
                self, "artifacts-bucket-key-arn-ssm", parameter_name=f"/AMC/KMS/ArtifactsBucketKeyArn"
            ),
        )
        self._artifacts_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "artifacts-bucket",
            bucket_arn=get_ssm_value(self, "artifacts-bucket-arn-ssm", parameter_name=f"/AMC/S3/ArtifactsBucketArn"),
        )

        self._artifacts_bucket_name = self._artifacts_bucket.bucket_name

    def _sync_zip_s3_bucket(self) -> None:
        
        bucket_deployment_role: Role = Role(
            self,
            f"{self._microservice_name}-s3-deployment-role",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )

        self._artifacts_key.grant_encrypt_decrypt(bucket_deployment_role)

        self._zip_path: str = "amc_quickstart/microservices/platform_management_notebooks"

        BucketDeployment(
            self,
            f"{self._microservice_name}-script-s3-deployment",
            sources=[Source.asset(f"{self._zip_path}/platform_manager")],
            destination_bucket=self._artifacts_bucket,
            destination_key_prefix=f"platform_notebook_manager_samples/platform_manager",
            server_side_encryption_aws_kms_key_id=self._artifacts_key.key_id,
            server_side_encryption=ServerSideEncryption.AWS_KMS,
            role=bucket_deployment_role,
        )

    def _create_sagemaker_kms_key(self) -> None:
        """kms key and alias"""

        self._kms_key_policy = PolicyDocument(
            statements=[PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:*"
                ],
                principals=[AccountRootPrincipal()],
                resources=["*"]
            ),
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:CreateGrant",
                    "kms:DescribeKey"
                ],
                principals=[AnyPrincipal()],
                resources=["*"],
                conditions={
                        "StringEquals": {
                            "kms:CallerAccount": f"{cdk.Aws.ACCOUNT_ID}",
                            "kms:ViaService": "sagemaker.amazonaws.com"
                        },
                        "Bool": {
                           "kms:GrantIsForAWSResource": "true"
                         }
                         }            
            )]
        )

        self._sagemaker_kms_key = KMSFactory.key(
            self,
            id=f"{self._microservice_name}-table-key",
            environment_id = self._environment_id,
            description=f"{self._microservice_name.title()} Table Key",
            alias=f"pmn-sagemaker-cmk",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
            policy=self._kms_key_policy
        )

        
    def _create_sagemaker_componenets(self) -> None:
        """Sagemaker role, lifecycle config and notebook instance"""
        
        
        sagemaker_role: Role = Role(
            self,
            f"{self._microservice_name}-role",
            assumed_by=ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")],
        )
        ManagedPolicy(
            self,
            f"{self._microservice_name}-policy",
            roles=[sagemaker_role],
            document=PolicyDocument(
                statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    resources=["arn:aws:s3:::*", "arn:aws:s3:::*/*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:GetItem"
                    ],
                    resources=[
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/{self._resource_prefix}-*",
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/tps-*",
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/wfm-*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "ssm:AddTagsToResource",
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:PutParameter",
                        "ssm:UpdateServiceSetting"
                    ],
                    resources=["*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey",
                        "kms:List*",
                        "kms:Describe*"
                    ],
                    resources=[f"arn:aws:kms:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:key/*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:wfm-*"],
                ),
                ]
            ),
        )

        self._artifacts_key.grant_encrypt_decrypt(sagemaker_role)

        sagemaker_lifecycle_config = sagemaker.CfnNotebookInstanceLifecycleConfig(
            self, 
            f"{self._microservice_name}-lc",
            notebook_instance_lifecycle_config_name=f"{self._microservice_name}-lc",
            on_start=[sagemaker.CfnNotebookInstanceLifecycleConfig.NotebookInstanceLifecycleHookProperty(
                content=cdk.Fn.base64(f"""
                #!/bin/bash
              
                set -e
                S3_BUCKET={self._artifacts_bucket_name}
                aws s3 sync s3://$S3_BUCKET/platform_notebook_manager_samples/ /home/ec2-user/SageMaker/
                chmod 777 /home/ec2-user/SageMaker/platform_manager
                """)
            )]
        )
      
        sagemaker.CfnNotebookInstance(
            self, 
            f"{self._microservice_name}-nb",
            instance_type='ml.t2.medium',
            role_arn=sagemaker_role.role_arn,
            kms_key_id=self._sagemaker_kms_key.key_id,
            lifecycle_config_name=sagemaker_lifecycle_config.attr_notebook_instance_lifecycle_config_name,
            notebook_instance_name=f"{self._resource_prefix}-quickstart-platform-manager-notebooks",
            root_access="Enabled",
            
        )


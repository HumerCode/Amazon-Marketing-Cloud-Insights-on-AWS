from typing import Any, Dict, List, Optional
import json
from aws_cdk.aws_kms import Key, IKey
from aws_cdk.core import Construct
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal, AccountRootPrincipal, AnyPrincipal
from aws_cdk import (core)
from aws_cdk.core import Construct, Stack, Fn, Duration, RemovalPolicy
import aws_cdk.aws_sagemaker as sagemaker
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_s3_deployment import BucketDeployment, ServerSideEncryption, Source
from aws_cdk.aws_ssm import StringParameter

def get_ssm_value(scope: Construct, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value
class PlatformManagerSageMaker(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment_id: str,
        team: str,
        microservice: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._environment_id: str = environment_id
        self._team = team
        self._microservice_name = microservice

        self._get_artifacts()
        self._sync_zip_s3_bucket()
        self._create_sagemaker_kms_key()
        self._create_sagemaker_componenets()

    def _get_artifacts(self) -> None:
        self._artifacts_key: IKey = Key.from_key_arn(
            self,
            "artifacts-bucket-key",
            key_arn=get_ssm_value(
                self, "artifacts-bucket-key-arn-ssm", parameter_name="/Orion/KMS/ArtifactsBucketKeyArn"
            ),
        )
        self._artifacts_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "artifacts-bucket",
            bucket_arn=get_ssm_value(self, "artifacts-bucket-arn-ssm", parameter_name="/Orion/S3/ArtifactsBucketArn"),
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

        self._zip_path: str = "orion_satellite/microservices/PlatformManagementNotebooks"

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
                            "kms:CallerAccount": f"{core.Aws.ACCOUNT_ID}",
                            "kms:ViaService": "sagemaker.amazonaws.com"
                        },
                        "Bool": {
                           "kms:GrantIsForAWSResource": "true"
                         }
                         }            
            )]
        )

        self._sagemaker_kms_key: Key = Key(
            self,
            id=f"{self._microservice_name}-table-key",
            description=f"{self._microservice_name.title()} Table Key",
            alias=f"sagemaker-demo-cm",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.DESTROY,
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
                        f"arn:aws:dynamodb:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/orion-*",
                        f"arn:aws:dynamodb:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/tps-*",
                        f"arn:aws:dynamodb:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/wfm-*"],
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
                    resources=[f"arn:aws:kms:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:key/*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[f"arn:aws:lambda:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:function:wfm-*"],
                ),
                ]
            ),
        )

        self._artifacts_key.grant_encrypt_decrypt(sagemaker_role)

        sagemaker_lifecycle_config = sagemaker.CfnNotebookInstanceLifecycleConfig(
            self, 
            f"{self._microservice_name}-lc",
            notebook_instance_lifecycle_config_name=f"{self._microservice_name}-lc",
            on_create=[sagemaker.CfnNotebookInstanceLifecycleConfig.NotebookInstanceLifecycleHookProperty(
                content=Fn.base64(f"""
                #!/bin/bash
              
                set -e
                S3_BUCKET={self._artifacts_bucket_name}
                aws s3 sync s3://$S3_BUCKET/platform_notebook_manager_samples/ /home/ec2-user/SageMaker/

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
            notebook_instance_name="saw-platform-manager",
            root_access="Enabled",
            
        )


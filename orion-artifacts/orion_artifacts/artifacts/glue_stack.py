#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any

from aws_cdk.aws_glue import CfnJob
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_kms import IKey, Key
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_s3_deployment import BucketDeployment, ServerSideEncryption, Source
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.core import Construct, Stack
import aws_cdk.aws_lakeformation as lakeformation


def get_ssm_value(scope: Construct, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class GlueStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._environment_id: str = environment_id
        self._glue_path: str = "orion_artifacts/glue/pyshell_scripts"
        self._get_artifacts()
        self._create_sdlf_glue_artifacts()
        self._create_sdlf_glue_jobs("demoteam", "amcdataset", f"{self._glue_path}/sdlf_heavy_transform/main.py")

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

    def _create_sdlf_glue_artifacts(self) -> None:

        bucket_deployment_role: Role = Role(
            self,
            "orion-glue-script-s3-deployment-role",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        self._artifacts_key.grant_encrypt_decrypt(bucket_deployment_role)
        BucketDeployment(
            self,
            "orion-glue-script-s3-deployment",
            sources=[Source.asset(f"{self._glue_path}/sdlf_heavy_transform")],
            destination_bucket=self._artifacts_bucket,
            destination_key_prefix=f"{self._glue_path}/sdlf_heavy_transform",
            server_side_encryption_aws_kms_key_id=self._artifacts_key.key_id,
            server_side_encryption=ServerSideEncryption.AWS_KMS,
            role=bucket_deployment_role,
        )
        self._glue_role: Role = Role(
            self,
            "orion-glue-stageb-job-role",
            assumed_by=ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        ManagedPolicy(
            self,
            "orion-glue-job-policy",
            roles=[self._glue_role],
            document=PolicyDocument(
                statements=[
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
                        resources=[
                            self.format_arn(
                                service="kms",
                                resource="key",
                                resource_name="*",
                                region="*",
                            )
                        ],
                        conditions={"ForAnyValue:StringLike": {"kms:ResourceAliases": "alias/orion-*-key"}},
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["s3:ListBucket"],
                        resources=[
                            self.format_arn(
                                resource=f"orion-{self._environment_id}-{self.region}-{self.account}-*",
                                service="s3",
                                region="",
                                account="",
                            ),
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                        ],
                        resources=[
                            self.format_arn(
                                resource=f"orion-{self._environment_id}-{self.region}-{self.account}-*",
                                service="s3",
                                resource_name="*",
                                region="",
                                account="",
                            ),
                        ],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lakeformation:*"
                        ],
                        resources=["*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["dynamodb:GetItem"],
                        resources=[
                            self.format_arn(
                                resource="table",
                                service="dynamodb",
                                resource_name=f"orion-{self._environment_id}-*",
                                region=f"{self.region}",
                                account=f"{self.account}",
                            ),
                            self.format_arn(
                                resource="table",
                                service="dynamodb",
                                resource_name=f"octagon-*",
                                region=f"{self.region}",
                                account=f"{self.account}",
                            ),
                        ],
                    ),
                ]
            ),
        )

    def _create_sdlf_glue_jobs(self, team, dataset, path) -> None:

        job: CfnJob = CfnJob(
            self,
            f"orion-heavy-transform-{team}-{dataset}-job",
            name=f"orion-{team}-{dataset}-glue-job",
            glue_version="2.0",
            allocated_capacity=2,
            execution_property=CfnJob.ExecutionPropertyProperty(max_concurrent_runs=4),
            command=CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self._artifacts_bucket.bucket_name}/{path}",
            ),
            default_arguments={"--job-bookmark-option": "job-bookmark-enable", "--enable-metrics": "", "--additional-python-modules": "awswrangler==2.4.0"},
            role=self._glue_role.role_arn,
        )
        StringParameter(
            self,
            f"orion-heavy-transform-{team}-{dataset}-job-name",
            parameter_name=f"/Orion/Glue/{team}/{dataset}/SDLFHeavyTranformJobName",
            string_value=job.name,  # type: ignore
        )
        
        StringParameter(
            self,
            f"orion-heavy-transform-{team}-{dataset}-job-role-arn",
            parameter_name=f"/Orion/IAM/{team}/{dataset}/HeavyTranformGlueRoleARN",
            string_value=self._glue_role.role_arn,  # type: ignore
        )

        cfn_data_lake_settings = lakeformation.CfnDataLakeSettings(self, "MyCfnDataLakeSettings",
            admins=[lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                data_lake_principal_identifier=self._glue_role.role_arn
            )])
    

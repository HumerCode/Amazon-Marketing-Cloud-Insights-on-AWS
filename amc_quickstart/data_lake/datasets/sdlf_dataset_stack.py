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

from os import pipe
from typing import Any, Optional
import json
from aws_cdk.aws_glue import CfnJob
from aws_cdk.aws_sqs import DeadLetterQueue, QueueEncryption
from aws_cdk.aws_glue import CfnDatabase
from aws_cdk.aws_iam import ServicePrincipal, PolicyDocument, PolicyStatement, Effect, ManagedPolicy, Role
from aws_cdk.aws_kms import IKey, Key
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_s3_deployment import BucketDeployment, ServerSideEncryption, Source
from aws_cdk.aws_lakeformation import CfnPermissions
import aws_cdk.aws_lakeformation as lakeformation
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_events import CfnRule
from aws_cdk.aws_lambda import CfnPermission
from aws_cdk.aws_ssm import StringParameter
from aws_ddk_core.base import BaseStack
import aws_cdk as cdk
from aws_ddk_core.resources import KMSFactory, SQSFactory


from ..utils import (
    RegisterConstruct
)

def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class SDLFDatasetStack(BaseStack):
    def __init__(self, scope, construct_id: str, environment_id: str, resource_prefix: str, params: dict, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)

        self._environment_id: str = environment_id
        self._params: dict = params
        self._org = self._params.get("org", "aws")
        self._app = self._params.get("app", "datalake")
        self._team = self._params.get("team", "demoteam")
        self._pipeline = self._params.get("pipeline", "adv")
        self._dataset = self._params.get("dataset", "amcdataset")
        self._resource_prefix = resource_prefix
    
        # Get analytics bucket props
        self._stage_bucket_key: IKey = Key.from_key_arn(
            self,
            "stage-bucket-key",
            key_arn=get_ssm_value(
                self,
                "stage-bucket-key-arn-ssm",
                parameter_name=f"/AMC/KMS/StageBucketKeyArn",
            ),
        )
        self._stage_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "stage-bucket",
            bucket_arn=get_ssm_value(
                self,
                "stage-bucket-arn-ssm",
                parameter_name=f"/AMC/S3/StageBucketArn",
            ),
        )

        self._glue_path = "amc_quickstart/foundations/glue/pyshell_scripts"
        self._get_artifacts()
        self._create_sdlf_glue_artifacts()
        self._create_sdlf_glue_jobs(team=self._team, 
                                    dataset=self._dataset, 
                                    path=f"{self._glue_path}/sdlf_heavy_transform/main.py")

        self._create_dataset(team=self._team, 
                            pipeline=self._pipeline, 
                            name=self._dataset,
                            stage_a_transform="amc_light_transform",
                            stage_b_transform="amc_heavy_transform")
    
    
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
        
    def _create_dataset(self, team: str, pipeline: str, name: str, stage_a_transform: Optional[str] = None, stage_b_transform: Optional[str] = None) -> None:
        
        self.stage_a_transform: str = stage_a_transform if stage_a_transform else "light_transform_blueprint"
        self.stage_b_transform: str = stage_b_transform if stage_b_transform else "heavy_transform_blueprint"

        self._props={
                "id":f"{team}-{name}",
                "description":f"{name.title()} dataset",
                "name": f"{team}-{name}",
                "type": "octagon_dataset",
                "pipeline": pipeline,
                "max_items_process": {
                    "stage_b": 100,
                    "stage_c": 100
                },
                "min_items_process" : {
                    "stage_b": 1,
                    "stage_c": 1
                },
                "version": 1,
                "transforms":{
                "stage_a_transform": self.stage_a_transform,
                "stage_b_transform": self.stage_b_transform,
            }
            }

        RegisterConstruct(self, self._props["id"], props=self._props)
        
        database: CfnDatabase = CfnDatabase(
            self,
            f"{self._resource_prefix}-{name}-database",
            database_input=CfnDatabase.DatabaseInputProperty(
                name=f"aws_datalake_{self._environment_id}_{team}_{name}_db"
            ),
            catalog_id=cdk.Aws.ACCOUNT_ID,
            
        )

        CfnPermissions(
            self,
            f"{self._resource_prefix}-{name}-glue-job-database-lakeformation-permissions",
            data_lake_principal=CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self._glue_role.role_arn
            ),
            resource=CfnPermissions.ResourceProperty(
                database_resource=CfnPermissions.DatabaseResourceProperty(name=database.ref)
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP"],
        )


        StringParameter(
            self,
            f"amc-{team}-{name}-stage-catalog",
            parameter_name=f"/AMC/Glue/{team}/{name}/StageDataCatalog",
            string_value=f"aws_datalake_{self._environment_id}_{team}_{name}_db",
        )

        #SQS and DLQ

        #sqs kms key resource
        sqs_key = KMSFactory.key(
            self,
            id=f"{self._resource_prefix}-{team}-{name}-sqs-key-b",
            environment_id = self._environment_id,
            description=f"{self._resource_prefix} SQS Key Stage B",
            alias=f"{self._resource_prefix}-{team}-{name}-sqs-stage-b-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        sqs_key_policy = PolicyDocument(
            statements=[PolicyStatement(
                actions=[
                    "kms:CreateGrant",
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:ReEncryptTo",
                    "kms:ReEncryptFrom",
                    "kms:ListAliases",
                    "kms:ListGrants",
                    "kms:ListKeys",
                    "kms:ListKeyPolicies"
                ],
                principals=[ServicePrincipal("lambda.amazonaws.com")],
                resources=["*"]
            )]
        )

        #SSM for sqs kms table arn
        StringParameter(
            self,
            f"amc-{team}-{name}-sqs-stage-b-key-arn-ssm",
            parameter_name=f"/AMC/KMS/SQS/{team}/{name}StageBKeyArn",
            string_value=sqs_key.key_arn,
        )

        #SSM for sqs kms table id
        StringParameter(
            self,
            f"amc-{team}-{name}-sqs-stage-b-key-id-ssm",
            parameter_name=f"/AMC/KMS/SQS/{team}/{name}StageBKeyId",
            string_value=sqs_key.key_id,
        )
        routing_dlq = DeadLetterQueue(
            max_receive_count=1, 
            queue=SQSFactory.queue(self, 
                            id=f'{self._resource_prefix}-{team}-{name}-dlq-b.fifo',
                            environment_id= self._environment_id,
                            queue_name=f'{self._resource_prefix}-{team}-{name}-dlq-b.fifo', 
                            fifo=True,
                            visibility_timeout=cdk.Duration.seconds(60),
                            encryption=QueueEncryption.KMS,
                            encryption_master_key=sqs_key))

        StringParameter(
            self,
            f'amc-{team}-{name}-dlq-b.fifo-ssm',
            parameter_name=f"/AMC/SQS/{team}/{name}StageBDLQ",
            string_value=f'{self._resource_prefix}-{team}-{name}-dlq-b.fifo',
        )


        routing_queue = SQSFactory.queue(
            self, 
            id=f'{self._resource_prefix}-{team}-{name}-queue-b.fifo', 
            environment_id = self._environment_id,
            queue_name=f'{self._resource_prefix}-{team}-{name}-queue-b.fifo', 
            fifo=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key, 
            dead_letter_queue=routing_dlq)

        StringParameter(
            self,
            f'amc-{team}-{name}-queue-b.fifo-ssm',
            parameter_name=f"/AMC/SQS/{team}/{name}StageBQueue",
            string_value=f'{self._resource_prefix}-{team}-{name}-queue-b.fifo',
        )


        #Eventbridge and event source mapping
        post_state_rule = CfnRule(
                    self, 
                    f"{self._resource_prefix}-{team}-{name}-rule-b",
                    name=f"{self._resource_prefix}-{team}-{name}-rule-b",
                    schedule_expression="cron(*/5 * * * ? *)",
                    state="ENABLED",
                    targets=[CfnRule.TargetProperty(
                        arn=f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._resource_prefix}-{team}-{pipeline}-routing-b",
                        id=f"{self._resource_prefix}-{team}-{name}-rule-b",
                        input=json.dumps({
                                "team": team,
                                "pipeline": pipeline,
                                "pipeline_stage": "StageB",
                                "dataset": name,
                                "org": self._org,
                                "app": self._app,
                                "env": self._environment_id
                            }, indent = 4)
                        )])

        Lambda_permissions = CfnPermission(self, f"{self._resource_prefix}-{team}-{name}-routing-b",
                        action="lambda:InvokeFunction",
                        function_name=f"{self._resource_prefix}-{team}-{pipeline}-routing-b",
                        principal="events.amazonaws.com",
                        source_arn=post_state_rule.attr_arn
                    )
 
    def _create_sdlf_glue_artifacts(self) -> None:

        bucket_deployment_role: Role = Role(
            self,
            f"{self._resource_prefix}-glue-script-s3-deployment-role",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        
        self._artifacts_key.grant_encrypt_decrypt(bucket_deployment_role)
        BucketDeployment(
            self,
            f"{self._resource_prefix}-glue-script-s3-deployment",
            sources=[Source.asset(f"{self._glue_path}/sdlf_heavy_transform")],
            destination_bucket=self._artifacts_bucket,
            destination_key_prefix=f"{self._glue_path}/sdlf_heavy_transform",
            server_side_encryption_aws_kms_key_id=self._artifacts_key.key_id,
            server_side_encryption=ServerSideEncryption.AWS_KMS,
            role=bucket_deployment_role,
        )
        self._glue_role: Role = Role(
            self,
            f"{self._resource_prefix}-glue-stageb-job-role",
            assumed_by=ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        
        ManagedPolicy(
            self,
            f"{self._resource_prefix}-glue-job-policy",
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
                            "kms:GenerateDataKey",
                            "kms:GenerateDataKeyPair",
                            "kms:GenerateDataKeyPairWithoutPlaintext",
                            "kms:GenerateDataKeyWithoutPlaintext",
                            "kms:ReEncryptTo",
                            "kms:ReEncryptFrom"
                        ],
                        resources=[
                            self.format_arn(
                                service="kms",
                                resource="key",
                                resource_name="*",
                                region="*",
                            )
                        ],
                        conditions={"ForAnyValue:StringLike": {"kms:ResourceAliases": f"alias/{self._resource_prefix}-*-key"}},
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
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                        ],
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
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lakeformation:DeregisterResource",
                            "lakeformation:GetDataAccess",
                            "lakeformation:GrantPermissions",
                            "lakeformation:PutDataLakeSettings",
                            "lakeformation:GetDataLakeSettings",
                            "lakeformation:RegisterResource",
                            "lakeformation:RevokePermissions",
                            "lakeformation:UpdateResource",
                            "glue:CreateDatabase",
                            "glue:CreateJob",
                            "glue:CreateSecurityConfiguration",
                            "glue:DeleteDatabase",
                            "glue:DeleteJob",
                            "glue:DeleteSecurityConfiguration",
                            "glue:GetDatabase",
                            "glue:GetDatabases",
                            "glue:GetMapping",
                            "glue:GetPartition",
                            "glue:GetPartitions",
                            "glue:GetPartitionIndexes",
                            "glue:GetSchema",
                            "glue:GetSchemaByDefinition",
                            "glue:GetSchemaVersion",
                            "glue:GetSchemaVersionsDiff",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:GetTableVersion",
                            "glue:GetTableVersions",
                            "glue:GetTags",
                            "glue:PutDataCatalogEncryptionSettings",
                            "glue:SearchTables",
                            "glue:TagResource",
                            "glue:UntagResource",
                            "glue:UpdateDatabase",
                            "glue:UpdateJob",
                            "glue:ListSchemas",
                            "glue:ListSchemaVersions"
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
                                resource_name=f"{self._resource_prefix}-{self._environment_id}-*",
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
            f"{self._resource_prefix}-heavy-transform-{team}-{dataset}-job",
            name=f"{self._resource_prefix}-{team}-{dataset}-glue-job",
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
            f"amc-heavy-transform-{team}-{dataset}-job-name",
            parameter_name=f"/AMC/Glue/{team}/{dataset}/SDLFHeavyTranformJobName",
            string_value=job.name,  # type: ignore
        )
        
        StringParameter(
            self,
            f"amc-heavy-transform-{team}-{dataset}-job-role-arn",
            parameter_name=f"/AMC/IAM/{team}/{dataset}/HeavyTranformGlueRoleARN",
            string_value=self._glue_role.role_arn,  # type: ignore
        )

        cfn_data_lake_settings = lakeformation.CfnDataLakeSettings(self, "MyCfnDataLakeSettings",
            admins=[lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                data_lake_principal_identifier=self._glue_role.role_arn
            )])
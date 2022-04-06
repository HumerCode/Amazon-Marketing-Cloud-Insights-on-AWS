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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
from aws_cdk.aws_kms import IKey, Key
from aws_cdk.aws_events import EventPattern, IRuleTarget
from aws_cdk.aws_events_targets import LambdaFunction
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_lambda import Code, LayerVersion, Runtime
from aws_ddk_core.pipelines.stage import DataStage
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_ssm import StringParameter
import aws_cdk as cdk
from aws_ddk_core.resources import LambdaFactory


from ..utils import (
    RegisterConstruct
)

def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

@dataclass
class SDLFHeavyTransformConfig:
    team: str
    pipeline: str


class SDLFHeavyTransform(DataStage):
    def __init__(
        self,
        scope,
        name: str,
        prefix: str,
        id: str,
        environment_id: str,
        config: SDLFHeavyTransformConfig,
        props: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, name, **kwargs)

        self._config: SDLFHeavyTransformConfig = config
        self._environment_id: str = environment_id
        self._props: Dict[str, Any] = props
        self._prefix = prefix

        RegisterConstruct(self, self._props["id"], props=self._props)

        self._stage_bucket_key: IKey = Key.from_key_arn(
            self,
            "stage-bucket-key",
            key_arn=get_ssm_value(
                self,
                "stage-bucket-key-arn-ssm",
                parameter_name="/AMC/KMS/StageBucketKeyArn",
            ),
        )
        self._stage_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "stage-bucket",
            bucket_arn=get_ssm_value(self, "stage-bucket-arn-ssm", parameter_name="/AMC/S3/StageBucketArn"),
        )

        self._data_lake_library_layer_arn = get_ssm_value(
                self,
                "data-lake-library-layer-arn-ssm",
                parameter_name="/AMC/Layer/DataLakeLibrary",
            )

        self.team = self._config.team
        self.pipeline = self._config.pipeline

        self._create_lambdas(self.team, self.pipeline)
        self._create_state_machine(name = f"{self._prefix}-{self.team}-{self.pipeline}-sm-b")


    def _create_lambdas(self, team, pipeline) -> None:

        self._routing_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-routing-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-routing-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/routing")),
            handler="handler.lambda_handler",
            description="Triggers Step Function stageB",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._redrive_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-redrive-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-redrive-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/redrive")),
            handler="handler.lambda_handler",
            environment={
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageB"
            },
            description="Redrive Step Function stageB",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._postupdate_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-postupdate-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-postupdate-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/postupdate-metadata")),
            handler="handler.lambda_handler",
            description="post update metadata",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._check_job_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-checkjob-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-checkjob-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/check-job")),
            handler="handler.lambda_handler",
            description="check if glue job still running",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._error_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-error-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-error-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/error")),
            handler="handler.lambda_handler",
            description="send errors to DLQ",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._process_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-process-b",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-process-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/process-object")),
            handler="handler.lambda_handler",
            description="exeute heavy transform",
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._stage_bucket_key.grant_decrypt(self._process_lambda)
        self._stage_bucket.grant_read(self._process_lambda)
        self._stage_bucket_key.grant_encrypt(self._process_lambda)
        self._stage_bucket.grant_write(self._process_lambda)
        
        wrangler_layer_version = LayerVersion.from_layer_version_arn(
            self,
            "wrangler-layer",
            layer_version_arn=cdk.Fn.import_value("aws-data-wrangler-py3-8"),
        )
        self._process_lambda.add_layers(wrangler_layer_version)
        self._check_job_lambda.add_layers(wrangler_layer_version)

        data_lake_layer_version = LayerVersion.from_layer_version_arn(
            self,
            f"{self._prefix}-layer-2",
            layer_version_arn=self._data_lake_library_layer_arn, 
        )
        
        for _lambda_object in [self._routing_lambda, self._postupdate_lambda, self._check_job_lambda,  self._process_lambda, self._error_lambda, self._redrive_lambda]:
            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "states:StartExecution"
                    ],
                    resources=[f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{self._prefix}-{team}-{pipeline}-sm-b"],
                )
            )

            _lambda_object.add_to_role_policy(
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
                        "kms:ReEncryptFrom",
                        "kms:ListAliases",
                        "kms:ListGrants",
                        "kms:ListKeys",
                        "kms:ListKeyPolicies"
                    ],
                    resources=["*"],
                    conditions={
                        "ForAnyValue:StringLike":{
                            "kms:ResourceAliases": [f"alias/{self._prefix}-*","alias/tps-*"]
                        }
                    }
                )
            )
            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "glue:StartCrawler"
                    ],
                    resources=[f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:crawler/{self._prefix}-{team}-*"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "glue:StartJobRun",
                        "glue:GetJobRun"
                    ],
                    resources=["*"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:ListBucket"
                    ],
                    resources=[self._stage_bucket.bucket_arn],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetObject"
                    ],
                    resources=[
                        f"arn:aws:s3:::{self._stage_bucket.bucket_name}/pre-stage/{team}/*",
                        f"arn:aws:s3:::{self._stage_bucket.bucket_name}/post-stage/{team}/*",
                    ],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:DescribeTable",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:ConditionCheckItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:GetRecords",
                        "dynamodb:ListTables",
                        "dynamodb:DescribeTable"
                    ],
                    resources=[
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/octagon-*",
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/{self._prefix}-*"
                    ],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters"
                    ],
                    resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/AMC/*"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sqs:SendMessage",
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:ListQueues",
                        "sqs:GetQueueUrl",
                        "sqs:ListDeadLetterSourceQueues",
                        "sqs:ListQueueTags"
                    ],
                    resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{self._prefix}-{team}-*"],
                )
            )


            _lambda_object.add_layers(data_lake_layer_version)

    def _create_state_machine(self, name) -> None:
    
    
        definition = {
                        "Comment": "Simple pseudo flow",
                        "StartAt": "Try",
                        "States": {
                            "Try": {
                            "Type": "Parallel",
                            "Branches": [
                                {
                                "StartAt": "Process Data",
                                "States":{
                                    "Process Data": {
                                    "Type": "Task",
                                    "Resource": self._process_lambda.function_arn,
                                    "Comment": "Process Data",
                                    "ResultPath": "$.body.job",
                                    "Next": "Wait"
                                    },
                                    "Wait": {
                                        "Type": "Wait",
                                        "Seconds": 15,
                                        "Next": "Get Job status"
                                    },
                                    "Get Job status": {
                                        "Type": "Task",
                                        "Resource": self._check_job_lambda.function_arn,
                                        "ResultPath": "$.body.job",
                                        "Next": "Did Job finish?"
                                    },
                                    "Did Job finish?": {
                                        "Type": "Choice",
                                        "Choices": [{
                                            "Variable": "$.body.job.jobDetails.jobStatus",
                                            "StringEquals": "SUCCEEDED",
                                            "Next": "Post-update Comprehensive Catalogue"
                                        },{
                                            "Variable": "$.body.job.jobDetails.jobStatus",
                                            "StringEquals": "FAILED",
                                            "Next": "Job Failed"
                                        }],
                                        "Default": "Wait"
                                    },
                                    "Job Failed": {
                                    "Type": "Fail",
                                    "Error": "Job Failed",
                                    "Cause": "Job failed, please check the logs"
                                    },
                                    "Post-update Comprehensive Catalogue": {
                                    "Type": "Task",
                                    "Resource": self._postupdate_lambda.function_arn,
                                    "Comment": "Post-update Comprehensive Catalogue",
                                    "ResultPath": "$.statusCode",
                                    "End": True
                                    },
                                }
                                }
                            ],
                            "Catch": [
                                    {
                                    "ErrorEquals": [ "States.ALL" ],
                                    "ResultPath": None,
                                    "Next": "Error"
                                    }
                                ],
                                "Next": "Done"
                                },
                                "Done": {
                                "Type": "Succeed"
                                },
                                "Error": {
                                "Type": "Task",
                                "Resource": self._error_lambda.function_arn,
                                "Comment": "Send Original Payload to DLQ",
                                "Next": "Failed"
                                },
                                "Failed": {
                                "Type": "Fail"
                        }
                        }
                        }



        sfn_role: Role = Role(
            self,
            f"{name}-sfn-job-role",
            assumed_by=ServicePrincipal("states.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                              ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )
        ManagedPolicy(
            self,
            f"{name}-sfn-job-policy",
            roles=[sfn_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "lambda:InvokeFunction"
                    ],
                    resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._prefix}-{self.team}-*"],
                )
                ]
            ),
        )

        sm_b = sfn.CfnStateMachine(
        self, 
        f'{name}-sm-b',
        role_arn = sfn_role.role_arn, 
        definition_string=json.dumps(definition, indent = 4), 
        state_machine_name=f"{name}")

        StringParameter(
            self,
            f"{name}",
            parameter_name=f"/AMC/SM/{self.team}/{self.pipeline}StageBSM",
            string_value=sm_b.attr_arn,
        )

    def get_event_pattern(self) -> Optional[EventPattern]:
        return None

    def get_targets(self) -> Optional[List[IRuleTarget]]:
        return [LambdaFunction(self._routing_lambda), ]

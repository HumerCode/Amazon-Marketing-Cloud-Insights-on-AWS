#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
from aws_cdk.aws_kms import IKey, Key
from aws_cdk.aws_events import EventPattern, IRuleTarget, RuleTargetInput
from aws_cdk.aws_events_targets import SfnStateMachine, LambdaFunction
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_lambda import Code, Function, LayerVersion
from aws_cdk.aws_stepfunctions import IntegrationPattern, JsonPath, StateMachine, TaskInput
from aws_cdk.aws_stepfunctions_tasks import GlueStartJobRun, LambdaInvoke
from aws_cdk.core import Construct, Duration
from orion_commons import LambdaFactory, StageConstruct, get_ssm_value
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk import (core)
from aws_cdk.core import Construct, Stack, Fn
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_ssm import StringParameter


@dataclass
class SDLFHeavyTransformConfig:
    team: str
    pipeline: str


class SDLFHeavyTransform(StageConstruct):
    def __init__(
        self,
        scope: Construct,
        pipeline_id: str,
        id: str,
        environment_id: str,
        config: SDLFHeavyTransformConfig,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, pipeline_id, id, **kwargs)

        self._config: SDLFHeavyTransformConfig = config
        self._environment_id: str = environment_id


        self._stage_bucket_key: IKey = Key.from_key_arn(
            self,
            "stage-bucket-key",
            key_arn=get_ssm_value(
                self,
                "stage-bucket-key-arn-ssm",
                parameter_name="/Orion/KMS/StageBucketKeyArn",
            ),
        )
        self._stage_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "stage-bucket",
            bucket_arn=get_ssm_value(self, "stage-bucket-arn-ssm", parameter_name="/Orion/S3/StageBucketArn"),
        )


        # self._didc_table_arn = get_ssm_value(
        #         self,
        #         "didc-table-arn-ssm",
        #         parameter_name="/Orion/DynamoDB/DidcTableArn",
        #     )

        self._orion_library_layer_arn = get_ssm_value(
                self,
                "orion-library-layer-arn-ssm",
                parameter_name="/Orion/Layer/OrionLibrary",
            )

        self.team = self._config.team
        self.pipeline = self._config.pipeline

        self._create_lambdas(self.team, self.pipeline)
        self._create_state_machine(name = f"orion-{self.team}-{self.pipeline}-sm-b")


    def _create_lambdas(self, team, pipeline) -> None:

        self._routing_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"orion-{team}-{pipeline}-routing-b",
            function_name=f"orion-{team}-{pipeline}-routing-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/routing")),
            handler="handler.lambda_handler",
            description="Triggers Step Function stageB",
            timeout=Duration.minutes(10),
        )

        self._redrive_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"orion-{team}-{pipeline}-redrive-b",
            function_name=f"orion-{team}-{pipeline}-redrive-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/redrive")),
            handler="handler.lambda_handler",
            environment={
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageB"
            },
            description="Redrive Step Function stageB",
            timeout=Duration.minutes(10),
        )

        self._postupdate_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"orion-{team}-{pipeline}-postupdate-b",
            function_name=f"orion-{team}-{pipeline}-postupdate-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/postupdate-metadata")),
            handler="handler.lambda_handler",
            description="post update metadata",
            timeout=Duration.minutes(10),
        )

        # self._crawl_lambda: Function = LambdaFactory.function(
        #     self,
        #     environment_id=self._environment_id,
        #     id=f"orion-{team}-{pipeline}-crawl-b",
        #     function_name=f"orion-{team}-{pipeline}-crawl-b",
        #     code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/crawl-data")),
        #     handler="handler.lambda_handler",
        #     description="Invoke crawler to catalog data",
        #     timeout=Duration.minutes(10),
        # )

        self._check_job_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"orion-{team}-{pipeline}-checkjob-b",
            function_name=f"orion-{team}-{pipeline}-checkjob-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/check-job")),
            handler="handler.lambda_handler",
            description="check if glue job still running",
            timeout=Duration.minutes(10),
        )

        self._error_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"orion-error-b",
            function_name=f"orion-{team}-{pipeline}-error-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/error")),
            handler="handler.lambda_handler",
            description="send errors to DLQ",
            timeout=Duration.minutes(10),
        )

        self._process_lambda: Function = LambdaFactory.function(
            self,
            environment_id=self._environment_id,
            id=f"orion-{team}-{pipeline}-process-b",
            function_name=f"orion-{team}-{pipeline}-process-b",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_heavy_transform/process-object")),
            handler="handler.lambda_handler",
            description="exeute heavy transform",
            timeout=Duration.minutes(15),
        )

        self._stage_bucket_key.grant_decrypt(self._process_lambda)
        self._stage_bucket.grant_read(self._process_lambda)
        self._stage_bucket_key.grant_encrypt(self._process_lambda)
        self._stage_bucket.grant_write(self._process_lambda)
        
        wrangler_layer_version = LayerVersion.from_layer_version_arn(
            self,
            "wrangler-layer",
            layer_version_arn=Fn.import_value("aws-data-wrangler-py3-8"),
        )
        self._process_lambda.add_layers(wrangler_layer_version)
        self._check_job_lambda.add_layers(wrangler_layer_version)

        orion_layer_version = LayerVersion.from_layer_version_arn(
            self,
            "orion-layer-2",
            layer_version_arn=self._orion_library_layer_arn,
        )

        for _lambda_object in [self._routing_lambda, self._postupdate_lambda, self._check_job_lambda,  self._process_lambda, self._error_lambda, self._redrive_lambda]:
            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "states:StartExecution"
                    ],
                    resources=[f"arn:aws:states:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:stateMachine:orion-{team}-{pipeline}-sm-b"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:*"
                    ],
                    resources=["*"],
                    conditions={
                        "ForAnyValue:StringLike":{
                            "kms:ResourceAliases": "alias/orion-*"
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
                    resources=[f"arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:crawler/orion-{team}-*"],
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
                        "dynamodb:*"
                    ],
                    resources=[
                        f"arn:aws:dynamodb:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/octagon-*",
                        f"arn:aws:dynamodb:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/orion-*"
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
                    resources=[f"arn:aws:ssm:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:parameter/Orion/*"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sqs:*"
                    ],
                    resources=[f"arn:aws:sqs:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:orion-{team}-*"],
                )
            )


            _lambda_object.add_layers(orion_layer_version)

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
                    resources=[f"arn:aws:lambda:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:function:orion-{self.team}-*"],
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
            parameter_name=f"/Orion/SM/{self.team}/{self.pipeline}StageBSM",
            string_value=sm_b.attr_arn,
        )

    def get_event_pattern(self) -> Optional[EventPattern]:
        return None

    def get_targets(self) -> Optional[List[IRuleTarget]]:
        return [LambdaFunction(self._routing_lambda), ]

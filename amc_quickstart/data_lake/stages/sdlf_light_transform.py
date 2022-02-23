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
from aws_cdk.aws_lambda import Code, Function, IFunction, LayerVersion, Runtime
from aws_ddk_core.pipelines.stage import Stage
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.aws_sqs import DeadLetterQueue, QueueEncryption
from aws_cdk.aws_lambda import EventSourceMapping
import aws_cdk as cdk
from aws_ddk_core.resources import KMSFactory, SQSFactory, LambdaFactory


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
class SDLFLightTransformConfig:
    team: str
    pipeline: str


class SDLFLightTransform(Stage):
    def __init__(
        self,
        scope,
        name: str,
        prefix:str,
        id: str,
        environment_id: str,
        config: SDLFLightTransformConfig,
        props: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, name, **kwargs)

        self._config: SDLFLightTransformConfig = config
        self._environment_id: str = environment_id
        self._prefix = prefix

        self._props: Dict[str, Any] = props

        RegisterConstruct(self, self._props["id"], props=self._props)

        self._raw_bucket_key: IKey = Key.from_key_arn(
            self,
            "raw-bucket-key",
            key_arn=get_ssm_value(
                self,
                "raw-bucket-key-arn-ssm",
                parameter_name=f"/AMC/KMS/RawBucketKeyArn",
            ),
        )
        self._raw_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "raw-bucket",
            bucket_arn=get_ssm_value(self, "raw-bucket-arn-ssm", parameter_name=f"/AMC/S3/RawBucketArn"),
        )

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
            bucket_arn=get_ssm_value(self, "stage-bucket-arn-ssm", parameter_name=f"/AMC/S3/StageBucketArn"),
        )

        self._foundation_routing_lambda: IFunction = Function.from_function_arn(
            self,
            "foundation-routing-function",
            function_arn=get_ssm_value(self, "foundation-routing-function-ssm", parameter_name=f"/AMC/Lambda/Routing"),
        )

        self._data_lake_library_layer_arn = get_ssm_value(
                self,
                "data-lake-library-layer-arn-ssm",
                parameter_name="/AMC/Layer/DataLakeLibrary",
            )
        

        self.team = self._config.team
        self.pipeline = self._config.pipeline

        self._create_lambdas(self.team, self.pipeline)
        self._create_state_machine(name = f"{self._prefix}-{self.team}-{self.pipeline}-sm-a")
        self._create_queue(self.team, self.pipeline)


    def _create_queue(self, team, pipeline) -> None:
        #SQS and DLQ
        #sqs kms key resource
        sqs_key = KMSFactory.key(
            self,
            f"{self._prefix}-{team}-{pipeline}-sqs-key-a",
            environment_id = self._environment_id,
            description=f"{self._prefix} SQS Key Stage A",
            alias=f"{self._prefix}-{team}-{pipeline}-sqs-stage-a-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        sqs_key_policy = PolicyDocument(
            statements=[PolicyStatement(
                actions=["kms:*"],
                principals=[ServicePrincipal("lambda.amazonaws.com")],
                resources=["*"]
            )]
        )

        #SSM for sqs kms table arn
        StringParameter(
            self,
            f"amc-{team}-{pipeline}-sqs-stage-a-key-arn-ssm",
            parameter_name=f"/AMC/KMS/SQS/{team}/{pipeline}StageAKeyArn",
            string_value=sqs_key.key_arn,
        )

        #SSM for sqs kms table id
        StringParameter(
            self,
            f"amc-{team}-{pipeline}-sqs-stage-a-key-id-ssm",
            parameter_name=f"/AMC/KMS/SQS/{team}/{pipeline}StageAKeyId",
            string_value=sqs_key.key_id,
        )

        self._routing_dlq = DeadLetterQueue(
            max_receive_count=1, 
            queue=SQSFactory.queue(self, 
                            id=f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo',
                            environment_id= self._environment_id,
                            queue_name=f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo', 
                            fifo=True,
                            visibility_timeout=cdk.Duration.seconds(60),
                            encryption=QueueEncryption.KMS,
                            encryption_master_key=sqs_key))

        StringParameter(
            self,
            f'amc-{team}-{pipeline}-dlq-a.fifo-ssm',
            parameter_name=f"/AMC/SQS/{team}/{pipeline}StageADLQ",
            string_value=f'{self._prefix}-{team}-{pipeline}-dlq-a.fifo',
        )


        self._routing_queue = SQSFactory.queue(
            self, 
            id=f'{self._prefix}-{team}-{pipeline}-queue-a.fifo', 
            environment_id= self._environment_id,
            queue_name=f'{self._prefix}-{team}-{pipeline}-queue-a.fifo', 
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=cdk.Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key, 
            dead_letter_queue=self._routing_dlq)

        

        StringParameter(
            self,
            f'amc-{team}-{pipeline}-queue-a.fifo-ssm',
            parameter_name=f"/AMC/SQS/{team}/{pipeline}StageAQueue",
            string_value=f'{self._prefix}-{team}-{pipeline}-queue-a.fifo',
        )

        event_source_mapping = EventSourceMapping(
                            self, 
                            "MyEventSourceMapping",
                            target=self._routing_lambda,
                            batch_size=10,
                            enabled=True,
                            event_source_arn=self._routing_queue.queue_arn,
                        )

    def _create_lambdas(self, team, pipeline) -> None:

        self._routing_lambda = LambdaFactory.function(
            self,
            "{self._prefix}-routing",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-routing-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_light_transform/routing")),
            handler="handler.lambda_handler",
            environment={
                "STEPFUNCTION": f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:sdlf-{team}-{pipeline}-sm-a"
            },
            description="Triggers Step Function",
            timeout=cdk.Duration.minutes(1),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._redrive_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-{team}-{pipeline}-redrive-a",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-redrive-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_light_transform/redrive")),
            handler="handler.lambda_handler",
            environment={
                "TEAM": self.team,
                "PIPELINE": self.pipeline,
                "STAGE": "StageA"
            },
            description="Redrive Step Function stageA",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )
        
        self._postupdate_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-post-update",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-postupdate-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_light_transform/postupdate-metadata")),
            handler="handler.lambda_handler",
            environment={
                "stage_bucket": f"{self._prefix}-{self._environment_id}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-stage"
            },
            description="post update metadata",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._preupdate_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-preupdate",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-preupdate-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_light_transform/preupdate-metadata")),
            handler="handler.lambda_handler",
            description="preupdate metadata",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._error_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-error-a",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-error-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_light_transform/error")),
            handler="handler.lambda_handler",
            description="send errors to DLQ",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
        )

        self._process_lambda = LambdaFactory.function(
            self,
            f"{self._prefix}-process",
            environment_id = self._environment_id,
            function_name=f"{self._prefix}-{team}-{pipeline}-process-a",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "lambdas/sdlf_light_transform/process-object")),
            handler="handler.lambda_handler",
            description="executes lights transform",
            timeout=cdk.Duration.minutes(15),
            memory_size=1536,
            runtime = Runtime.PYTHON_3_8,
        )

        self._raw_bucket_key.grant_decrypt(self._process_lambda)
        self._raw_bucket.grant_read(self._process_lambda)
        self._stage_bucket_key.grant_encrypt(self._process_lambda)
        self._stage_bucket.grant_write(self._process_lambda)

        wrangler_layer_version = LayerVersion.from_layer_version_arn(
            self,
            "wrangler-layer",
            layer_version_arn=cdk.Fn.import_value("aws-data-wrangler-py3-8"),
        )
        self._process_lambda.add_layers(wrangler_layer_version)

        self._process_lambda.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "s3:Get*",
                    "s3:List*",
                    "s3-object-lambda:Get*",
                    "s3-object-lambda:List*"
                ],
                resources=["*"],
                )
        )

        data_lake_layer_version = LayerVersion.from_layer_version_arn(
            self,
            f"{self._prefix}-layer-1",
            layer_version_arn=self._data_lake_library_layer_arn, 
        )

        for _lambda_object in [self._routing_lambda, self._postupdate_lambda, self._preupdate_lambda, self._process_lambda, self._error_lambda, self._redrive_lambda]:
            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{self._prefix}-{team}-{pipeline}-sm-a"],
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
                            "kms:ResourceAliases": [f"alias/{self._prefix}-*","alias/tps-*"]
                        }
                    }
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:ListBucket",
                        "s3:GetBucketVersioning"
                    ],
                    resources=[f"arn:aws:s3:::{self._prefix}-dev-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}-*"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetObject"
                    ],
                    resources=[f"arn:aws:s3:::{self._stage_bucket.bucket_name}/pre-stage/{team}/*"],
                )
            )

            _lambda_object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:*"
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
                        "sqs:*"
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
                            "StartAt": "Pre-update Comprehensive Catalogue",
                            "States": {
                                "Pre-update Comprehensive Catalogue": {
                                "Type": "Task",
                                "Resource": self._preupdate_lambda.function_arn,
                                "Comment": "Pre-update Comprehensive Catalogue",
                                "Next": "Execute Light Transformation"
                                },
                                "Execute Light Transformation": {
                                "Type": "Task",
                                "Resource": self._process_lambda.function_arn,
                                "Comment": "Execute Light Transformation",
                                "ResultPath": "$.body.processedKeys",
                                "Next": "Post-update comprehensive Catalogue"
                                },
                                "Post-update comprehensive Catalogue": {
                                "Type": "Task",
                                "Resource": self._postupdate_lambda.function_arn,
                                "Comment": "Post-update comprehensive Catalogue",
                                "ResultPath": "$.statusCode",
                                "End": True
                                }
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
                    resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._prefix}-{self.team}-{self.pipeline}-*"],
                )
                ]
            ),
        )

        sm_a = sfn.CfnStateMachine(
        self, 
        f'{name}-sm-a',
        role_arn = sfn_role.role_arn, 
        definition_string=json.dumps(definition, indent = 4), 
        state_machine_name=f"{name}")

        StringParameter(
            self,
            f"{name}",
            parameter_name=f"/AMC/SM/{self.team}/{self.pipeline}StageASM",
            string_value=sm_a.attr_arn,
        )

    def get_event_pattern(self) -> Optional[EventPattern]:
        return None

    def get_targets(self) -> Optional[List[IRuleTarget]]:
        return [LambdaFunction(self._foundation_routing_lambda), ]


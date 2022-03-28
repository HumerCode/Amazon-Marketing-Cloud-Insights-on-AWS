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
from typing import Any, Dict, List, Optional
import json
from aws_cdk.aws_kms import Key, IKey
from aws_cdk.aws_lambda import Code, LayerVersion, StartingPosition, EventSourceMapping, Runtime
from aws_ddk_core.base import BaseStack
import aws_cdk as cdk
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.aws_sns import Topic, Subscription, SubscriptionProtocol
import aws_cdk.aws_dynamodb as DDB
from aws_cdk.aws_sqs import DeadLetterQueue, QueueEncryption
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_s3_deployment import BucketDeployment, ServerSideEncryption, Source
from aws_ddk_core.resources import KMSFactory, SQSFactory, LambdaFactory


def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value


class TenantProvisiongService(BaseStack):
    def __init__(
        self,
        scope,
        construct_id: str,
        environment_id: str,
        microservice: str,
        team: str,
        resource_prefix: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)

        self._environment_id: str = environment_id
        self._microservice_name = microservice
        self._region = f"{cdk.Aws.REGION}"
        self._team = team
        self._resource_prefix=resource_prefix
        
        self._data_lake_library_layer_arn = get_ssm_value(
            self,
            "data-lake-library-layer-arn-ssm",
            parameter_name=f"/AMC/Layer/DataLakeLibrary",
        )
            
        self._powertools_layer = LayerVersion.from_layer_version_arn(
            self,
            id="lambda-powertools",
            layer_version_arn=f"arn:aws:lambda:{self._region}:017000801446:layer:AWSLambdaPowertoolsPython:7"
        )

        self._artifacts_bucket_key: IKey = Key.from_key_arn(
            self,
            "artifacts-bucket-key",
            key_arn=get_ssm_value(
                self,
                "artifacts-bucket-key-arn-ssm",
                parameter_name=f"/AMC/KMS/ArtifactsBucketKeyArn",
            ),
        )

        self._artifacts_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "artifacts-bucket",
            bucket_arn=get_ssm_value(self, "artifacts-bucket-arn-ssm", parameter_name=f"/AMC/S3/ArtifactsBucketArn"),
        )

        self._artifacts_bucket_name = self._artifacts_bucket.bucket_name

        self._sync_cfn_template_s3_bucket()

        self._create_sns_sqs_key()
        
        self._create_ddb_table(ddb_name=f"{self._microservice_name}-{self._team}-CustomerConfig-{self._environment_id}")

        self._create_sqs_queue(sqs_name=f"{self._microservice_name}-{self._team}-{self._environment_id}-CustomerAMCInstanceOnboarding")
        
        self._create_sns_topic(topic_name=f"{self._microservice_name}-{self._team}-CustomerConfig-SNSTopic-{self._environment_id}.fifo")
        
        self._create_amc_onboarding_sm(self._microservice_name,self._team, self._environment_id)

        self._create_lambdas(self._microservice_name,self._team, self._environment_id)
    
    # Move AMC Initialize Template to S3 Artifacts
    def _sync_cfn_template_s3_bucket(self) -> None:
        
        bucket_deployment_role: Role = Role(
            self,
            f"{self._microservice_name}-s3-deployment-role",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
        )

        self._artifacts_bucket_key.grant_encrypt_decrypt(bucket_deployment_role)

        self._template_path: str = "amc_quickstart/microservices/customer_management_service"

        BucketDeployment(
            self,
            f"{self._microservice_name}-script-s3-deployment",
            sources=[Source.asset(f"{self._template_path}/scripts")],
            destination_bucket=self._artifacts_bucket,
            destination_key_prefix=f"{self._microservice_name}/scripts/{self._team}",
            server_side_encryption_aws_kms_key_id=self._artifacts_bucket_key.key_id,
            server_side_encryption=ServerSideEncryption.AWS_KMS,
            role=bucket_deployment_role,
        )
    
    # Create SNS SQS KMS Integration Key
    def _create_sns_sqs_key(self):
        # KMS Key Creation
        self._sns_sqs_key = KMSFactory.key(
            self,
            f"{self._team}-{self._microservice_name}-sns-sqs-amc-key",
            environment_id = self._environment_id,
            description="KMS Key for AMC Onboarding bucket SNS topic & SQS queue",
            alias=f"{self._resource_prefix}-{self._microservice_name}-sqs-sns-ingegration-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        
        # KMS Key Policy
        self._sns_sqs_key.add_to_resource_policy(
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
                principals=[
                    ServicePrincipal("s3.amazonaws.com"),
                    ServicePrincipal("sqs.amazonaws.com"),
                    ServicePrincipal("sns.amazonaws.com")
                ]
            )
        )

    #    SSM Parameter for sqs sns kms arn
        StringParameter(
            self,
            f"{self._resource_prefix}-{self._microservice_name}-sqs-sns-ingegration-key",
            parameter_name=f"/AMC/KMS/{self._team}/{self._microservice_name}/SQSSNSKMSKey",
            string_value=self._sns_sqs_key.key_arn,
        )

    
    # DDB
    def _create_ddb_table(self, ddb_name):

        ddb_props={
            "partition_key": DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING),
            "sort_key": DDB.Attribute(name="customerName", type=DDB.AttributeType.STRING)
        }

        self._table: DDB.Table = DDB.Table(
            self,
            f"{ddb_name}-table",
            table_name = ddb_name,
            encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self._sns_sqs_key,
            stream=DDB.StreamViewType.NEW_AND_OLD_IMAGES,
            billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
            removal_policy= cdk.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
            **ddb_props,
        )

        StringParameter(
            self,
            f"{ddb_name}-table-arn-ssm",
            parameter_name=f"/AMC/DynamoDB/{self._microservice_name}/{self._team}/CustomerConfigTableArn",
            string_value=self._table.table_arn,
        )

        #SQS
    def _create_sqs_queue(self,sqs_name):
        dlq_name = f"{sqs_name}-queue-DLQ.fifo"
        queue_name = f"{sqs_name}-queue.fifo"

        self._onboarding_dlq = DeadLetterQueue(
            max_receive_count=10, 
            queue=SQSFactory.queue(self, 
                            id=dlq_name,
                            environment_id = self._environment_id,
                            queue_name=dlq_name, 
                            fifo=True,
                            visibility_timeout=cdk.Duration.seconds(30),
                            encryption=QueueEncryption.KMS,
                            encryption_master_key=self._sns_sqs_key,
                            content_based_deduplication=True,
                            delivery_delay=cdk.Duration.seconds(10)))

        StringParameter(
            self,
            f"{self._resource_prefix}-{sqs_name}-queue-DLQ.fifo",
            parameter_name=f"/AMC/SQS/{self._microservice_name}/{self._team}/CustomerAMCInstanceOnboardingDLQ",
            string_value=dlq_name,
        )

        self._onboarding_queue = SQSFactory.queue(
            self, 
            id=queue_name, 
            environment_id = self._environment_id,
            queue_name=queue_name, 
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=cdk.Duration.seconds(30),
            encryption=QueueEncryption.KMS,
            encryption_master_key=self._sns_sqs_key, 
            delivery_delay=cdk.Duration.seconds(10),
            dead_letter_queue=self._onboarding_dlq)

        # Queue Policy
        self._onboarding_queue.add_to_resource_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sqs:SendMessage"
                    ],
                    resources=[self._onboarding_queue.queue_arn],
                    principals=[ServicePrincipal("sns.amazonaws.com")]
                )
            )

        StringParameter(
            self,
            f'{self._resource_prefix}-{sqs_name}-queue.fifo',
            parameter_name=f"/AMC/SQS/{self._microservice_name}/{self._team}/CustomerAMCInstanceOnboardingQueue",
            string_value=queue_name,
        )

    # SNS
    def _create_sns_topic(self, topic_name):

        self._sns_topic = Topic(
            self,
            topic_name,
            topic_name=topic_name,
            content_based_deduplication=True,
            fifo=True,
            master_key=self._sns_sqs_key
        )

        sns_subscription_queue = Subscription(
            self,
            "Subscription",
            topic = self._sns_topic,
            endpoint=self._onboarding_queue.queue_arn,
            protocol=SubscriptionProtocol.SQS
        )

        #SSM for sns topic
        StringParameter(
            self,
            f"{self._resource_prefix}-{self._microservice_name}-{self._team}-sns-customer-config-topic-ssm",
            parameter_name=f"/AMC/SNS/{self._microservice_name}/{self._team}/CustomerConfigTopicName",
            string_value=topic_name,
        )

    def _create_lambdas(self,microservice, team, environment_id):

        self._customer_config_ddb_trigger = LambdaFactory.function(
            self,
            f"{self._resource_prefix}-{microservice}-{team}-CustomerConfigDynamoDBTrigger-{environment_id}",
            environment_id = self._environment_id,
            function_name=f"{self._resource_prefix}-{microservice}-{team}-CustomerConfigDynamoDBTrigger-{environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "customer_management_service/lambdas/tps/customer_config_ddb")),
            handler="handler.lambda_handler",
            description="An Amazon DynamoDB trigger that pushes the updates made to the customer config table to an SNS topic",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            environment={
                "SNS_TOPIC_ARN":self._sns_topic.topic_arn
            }
        )

        #  Lambda Role CustomerConfigDynamoDBTrigger
        self._customer_config_ddb_trigger.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey"
                ],
                resources=[self._sns_sqs_key.key_arn],
            )
        )
        self._customer_config_ddb_trigger.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "dynamodb:ListTables",
                    "dynamodb:ListGlobalTables",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:GetRecords",
                    "dynamodb:GetShardIterator",
                    "dynamodb:DescribeStream",
                    "dynamodb:ListStreams",
                    "dynamodb:ListShards"
                ],
                resources=[
                    f"arn:aws:dynamodb:*:*:table/{self._table.table_name}",
                    f"arn:aws:dynamodb:*:*:table/{self._table.table_name}/*",
                    "arn:aws:dynamodb:*:*:table/wfm-*",
                    "arn:aws:dynamodb:*:*:table/tps-*",
                    "arn:aws:dynamodb:*:*:table/sas-*"
                ]
            )
        )
        self._customer_config_ddb_trigger.add_to_role_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=["sns:Publish"],
                resources=[self._sns_topic.topic_arn]
            )
        )
        self._customer_config_ddb_trigger.add_layers(self._powertools_layer)

        self._customer_config_ddb_trigger.add_event_source_mapping(
            "lambda-ddb-event-source-mapping",
            batch_size=100,
            event_source_arn=self._table.table_stream_arn,
            starting_position=StartingPosition.TRIM_HORIZON,
            retry_attempts=1
        )

        self._lambda_amc_instance_setup = LambdaFactory.function(
            self,
            f"{microservice}-{team}-TriggerAMCSetupStepFunction-{environment_id}",
            environment_id = self._environment_id,
            function_name=f"{microservice}-{team}-TriggerAMCSetupStepFunction-{environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "customer_management_service/lambdas/tps/amc_instance_setup")),
            handler="handler.lambda_handler",
            description="Trigger AMC Instance setup state machine",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            runtime = Runtime.PYTHON_3_8,
            environment={
                "STATE_MACHINE_ARN":self._sm.attr_arn
            },
        )

        # rRoleLambdaAMCInstanceStateMachine
        self._lambda_amc_instance_setup.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey"
                    ],
                    resources=[self._sns_sqs_key.key_arn],
                )
            )
        self._lambda_amc_instance_setup.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=["states:StartExecution"],
                    resources=[
                        f"arn:aws:states:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stateMachine:{microservice}-{team}-*"
                    ],
                )
            )
        self._lambda_amc_instance_setup.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sqs:ListQueues",
                        "sqs:ListDeadLetterSourceQueues",
                        "sqs:ListQueueTags",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl"
                        ],
                    resources=[
                        f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{microservice}-{team}-{environment_id}-*"
                    ],
                )
            )
        self._lambda_amc_instance_setup.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters"
                    ],
                    resources=[
                        f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/AMC/*"
                    ],
                )
            )
        self._lambda_amc_instance_setup.add_layers(self._powertools_layer)

        event_source_mapping = EventSourceMapping(
                            self, 
                            "MyEventSourceMapping",
                            target=self._lambda_amc_instance_setup,
                            batch_size=10,
                            enabled=True,
                            event_source_arn=self._onboarding_queue.queue_arn,
                        )

    def _create_amc_onboarding_sm(self, microservice, team, environment_id):

        data_lake_layer_version = LayerVersion.from_layer_version_arn(
            self,
            f"{self._resource_prefix}-layer-1",
            layer_version_arn=self._data_lake_library_layer_arn, 
        )
        add_amc_instance_policy=PolicyDocument(
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:CreateBucket",
                        "s3:ListBucket",
                        "s3:PutBucketPolicy",
                        "s3:PutBucketAcl",
                        "s3:PutBucketPublicAccessBlock",
                        "s3:PutAccountPublicAccessBlock",
                        "s3:GetAccountPublicAccessBlock",
                        "s3:GetBucketPublicAccessBlock",
                        "s3:PutBucketEncryption",
                        "s3:PutBucketNotification",
                        "s3:PutBucketNotificationConfiguration",
                        "s3:PutBucketTagging",
                        "s3:SetBucketEncryption",
                        "s3:GetBucketAcl",
                        "s3:GetBucketNotification",
                        "s3:GetBucketEncryption",
                        "s3:GetEncryptionConfiguration",
                        "s3:PutEncryptionConfiguration",
                        "s3:GetBucketPolicy",
                        "s3:GetBucketPolicyStatus",
                        "s3:DeleteBucketPolicy"
                    ],
                    resources=["*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:DeleteBucket"
                    ],
                    resources=["arn:aws:s3:::amc*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    resources=[f"arn:aws:s3:::*/{microservice}/*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:CreateKey"
                    ],
                    resources=["*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:TagResource",
                        "kms:CreateAlias",
                        "kms:UpdateAlias",
                        "kms:DescribeKey",
                        "kms:PutKeyPolicy",
                        "kms:ScheduleKeyDeletion"
                    ],
                    resources=[
                        f"arn:aws:kms:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:key/*",
                        f"arn:aws:kms:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:alias/*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "iam:PassRole"
                    ],
                    resources=[
                        f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/service-role/{self._microservice_name}*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "cloudformation:GetTemplate",
                        "cloudformation:GetTemplateSummary",
                        "cloudformation:ListStacks",
                        "cloudformation:ValidateTemplate"
                    ],
                    resources=["*"],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "cloudformation:CreateChangeSet",
                        "cloudformation:CreateStack",
                        "cloudformation:DeleteChangeSet",
                        "cloudformation:DeleteStack",
                        "cloudformation:DescribeChangeSet",
                        "cloudformation:DescribeStacks",
                        "cloudformation:ExecuteChangeSet",
                        "cloudformation:SetStackPolicy",
                        "cloudformation:UpdateStack"
                    ],
                    resources=[
                        f"arn:aws:cloudformation:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stack/{self._resource_prefix}-*",
                        f"arn:aws:cloudformation:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stack/{self._microservice_name}-*",
                        f"arn:aws:cloudformation:{cdk.Aws.REGION}:aws:transform/*"
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "events:PutTargets",
                        "events:PutPermission",
                        "events:PutPartnerEvents",
                        "events:PutRule",
                        "events:PutEvents",
                        "events:CreatePartnerEventSource",
                        "events:CreateEventBus",
                        "events:CreateApiDestination",
                        "events:CreateArchive",
                        "events:CreateConnection",
                        "events:ListRuleNamesByTarget",
                        "events:ListRules",
                        "events:ListTargetsByRule",
                        "events:ListTagsForResource",
                        "events:ListEventSources",
                        "events:ListEventBuses",
                        "events:DescribeEventSource",
                        "events:DescribeEventBus",
                        "events:DescribeRule",
                        "events:EnableRule",
                        "event:ActivateEventSource",
                        "event:DeactivateEventSource",
                        "event:DeleteRule",
                        "event:RemoveTargets",
                        "lambda:AddPermission",
                        "lambda:RemovePermission"
                    ],
                    resources=["*"],
                )
            ]
        )

        add_amc_instance_role = Role(
            self,
            "Add AMC Instance Role",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={"AddAMCPolicy": add_amc_instance_policy},
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )

        self._add_amc_instance = LambdaFactory.function(
            self,
            f"{microservice}-{team}-AddAmcInstance-{environment_id}",
            environment_id = self._environment_id,
            function_name=f"{microservice}-{team}-AddAmcInstance-{environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "customer_management_service/lambdas/tps/add_amc_instance")),
            handler="handler.lambda_handler",
            description="Onboard AMC into environment",
            timeout=cdk.Duration.minutes(10),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
            role = add_amc_instance_role,
            environment={
                "templateUrl": f"https://{self._artifacts_bucket.bucket_name}.s3.amazonaws.com/{self._microservice_name}/scripts/{self._team}/amc-initialize.yaml",
                "lambdaRoleArn": add_amc_instance_role.role_arn,
                "Prefix": self._resource_prefix,
                "ENV": self._environment_id
            },
        )
        self._artifacts_bucket_key.grant_decrypt(self._add_amc_instance)

        self._add_amc_instance_check = LambdaFactory.function(
            self,
            f"{microservice}-{team}-AddAmcInstanceStatusCheck-{environment_id}",
            environment_id = self._environment_id,
            function_name=f"{microservice}-{team}-AddAmcInstanceStatusCheck-{environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "customer_management_service/lambdas/tps/add_amc_instance_check")),
            handler="handler.lambda_handler",
            description="Checks if stack has finished (success/failure)",
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            runtime = Runtime.PYTHON_3_8,
            role = add_amc_instance_role
        )

        self._amc_instance_post_deploy_metadata = LambdaFactory.function(
            self,
            f"{microservice}-{team}-postDeployMetadataInstanceConfig-{environment_id}",
            environment_id = self._environment_id,
            function_name=f"{microservice}-{team}-postDeployMetadataInstanceConfig-{environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "customer_management_service/lambdas/tps/amc_instance_post_deploy_metadata")),
            handler="handler.lambda_handler",
            description="Onboard AMC into environment",
            timeout=cdk.Duration.minutes(10),
            memory_size=512,
            runtime = Runtime.PYTHON_3_8,
            environment={
                "AccountId": cdk.Aws.ACCOUNT_ID,
                "Region": cdk.Aws.REGION,
                "Prefix": self._resource_prefix,
                "ENV": self._environment_id
            },
        )
        
        self._amc_instance_post_deploy_metadata.add_layers(data_lake_layer_version)

        self._amc_instance_post_deploy_metadata.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    resources=[f"arn:aws:s3:::*/{microservice}/*"],
                )
            )
        self._amc_instance_post_deploy_metadata.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "s3:CreateBucket",
                        "s3:PutBucketPolicy",
                        "s3:ListBucket"
                    ],
                    resources=["*"],
                )
            )
        self._amc_instance_post_deploy_metadata.add_to_role_policy(
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
                            "kms:ResourceAliases": f"alias/{self._resource_prefix}-*"
                        }
                    }
                )
            )
        self._amc_instance_post_deploy_metadata.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:BatchGetItem",
                        "dynamodb:DescribeTable",
                        "dynamodb:GetItem",
                        "dynamodb:GetRecords",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:PutItem"
                    ],
                    resources=[
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/{self._resource_prefix}-*",
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/wfm-*",
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/tps-*",
                        f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/sas-*"
                    ],
                )
            )

        definition = {
            "Comment": "Simple pseudo flow",
            "StartAt": "Try",
            "States": {
                "Try": {
                "Type": "Parallel",
                "Branches": [
                    {
                    "StartAt": "Process AMC Instance Request",
                    "States": {
                        "Process AMC Instance Request": {
                        "Type": "Task",
                        "Resource": self._add_amc_instance.function_arn,
                        "Comment": "Process AMC Instance Request",
                        "ResultPath": "$.body.stackId",
                        "Next": "Wait"
                        },
                        "Wait": {
                            "Type": "Wait",
                            "Seconds": 45,
                            "Next": "Get Stack status"
                        },
                        "Get Stack status": {
                            "Type": "Task",
                            "Resource": self._add_amc_instance_check.function_arn,
                            "ResultPath": "$.body.stackStatus",
                            "Next": "Did Job finish?"
                        },
                        "Did Job finish?": {
                            "Type": "Choice",
                            "Choices": [{
                                "Variable": "$.body.stackStatus",
                                "StringEquals": "CREATE_COMPLETE",
                                "Next": "Post-deploy update sdlf config"
                            },{
                                "Variable": "$.body.stackStatus",
                                "StringEquals": "UPDATE_COMPLETE",
                                "Next": "Post-deploy update sdlf config"
                            },{
                                "Variable": "$.body.stackStatus",
                                "StringEquals": "FAILED",
                                "Next": "Stack Failed"
                            }],
                            "Default": "Wait"
                        },
                        "Stack Failed": {
                        "Type": "Fail",
                        "Error": "Stack Failed",
                        "Cause": "Stack failed, please check the logs"
                        },
                        "Post-deploy update sdlf config": {
                        "Type": "Task",
                        "Resource": self._amc_instance_post_deploy_metadata.function_arn,
                        "Comment": "Post-deploy update sdlf config",
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
                    "Next": "Failed"
                    }
                ],
                "Next": "Done"
                },
                "Done": {
                "Type": "Succeed"
                },
                "Failed": {
                "Type": "Fail"
                }
            }
        }

        name = f"{microservice}-{team}-initialize-amc"
        sfn_role: Role = Role(
            self,
            f"{name}-sfn-job-role",
            assumed_by=ServicePrincipal("states.amazonaws.com"),
            managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
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
                    resources=[
                        f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._resource_prefix}-tps-*",
                        f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:tps-*"
                    ],
                    ),
                    PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "states:DescribeExecution",
                        "states:StopExecution"
                    ],
                    resources=["*"],
                    ),
                    PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "events:DescribeRule",
                        "events:PutTargets",
                        "events:PutRule"
                    ],
                    resources=[
                        f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule",
                    ],
                    )
                ]
            )
        )

        self._sm = sfn.CfnStateMachine(
            self, 
            name,
            role_arn = sfn_role.role_arn, 
            definition_string=json.dumps(definition, indent = 4), 
            state_machine_name=name
        )

        StringParameter(
            self,
            f"{microservice}-{team}-initialize-amc-sm",
            parameter_name=f"/AMC/SM/{microservice}/{team}/InitializeAMC",
            string_value=self._sm.attr_arn
        ) 

        # Lambda COMMON POLICY
        for _object in [self._amc_instance_post_deploy_metadata, self._add_amc_instance, self._add_amc_instance_check]:
            _object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:PutParameter",
                        "ssm:DeleteParameter",
                        "ssm:AddTagsToResource"
                    ],
                    resources=[f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/AMC/*"],
                )
            )
            
            _object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "sns:GetTopicAttributes",
                        "sns:CreateTopic",
                        "sns:ListTopics",
                        "sns:SetTopicAttributes",
                        "sns:DeleteTopic"
                    ],
                    resources=[self._sns_topic.topic_arn],
                )
            )

            _object.add_to_role_policy(
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:TagResource",
                        "kms:CreateAlias",
                        "kms:UpdateAlias",
                        "kms:DescribeKey",
                        "kms:PutKeyPolicy",
                        "kms:ScheduleKeyDeletion"
                    ],
                    resources=[self._sns_sqs_key.key_arn],
                )
            )

            _object.add_layers(self._powertools_layer)
            
